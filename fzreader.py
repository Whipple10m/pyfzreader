import struct

class FZReader:
    def __init__(self, filename, verbose=False) -> None:
        self.filename = filename
        self.file = None
        self.pdata = b''
        self.NWTOLR = 0
        self.verbose = verbose
        pass

    def __enter__(self):
        self.file = open(self.filename, 'rb')
        self.pdata = b''
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()

    def _nio(self, iocb):
        if(iocb < 12):
            return 1;
        else:
            return iocb&0xFFFF - 12;

    def _read_pdata(self):
        self.pdata = self.file.read(4*4)
        if(len(self.pdata) == 0):
            return
        if(len(self.pdata) != 16):
            raise RuntimeError('ZEBRA physical record MAGIC could not be read')
        if(struct.unpack('>IIII',self.pdata) != (0x0123CDEF,0x80708070,0x4321ABCD,0x80618061)):
            raise RuntimeError('ZEBRA physical record MAGIC not found')

        self.pdata = self.file.read(4*4)
        if(len(self.pdata) != 16):
            raise RuntimeError('ZEBRA physical record header could not be read')
        NWPHR, PRC, self.NWTOLR, NFAST = struct.unpack('>IIII',self.pdata)
        NWPHR = NWPHR & 0xFFFFFF
        if(self.verbose):
            print(f"PH: NWPHR={NWPHR}, PRC={PRC}, NWTOLR={self.NWTOLR}, NFAST={NFAST}")

        self.pdata = self.file.read((NWPHR*(1+NFAST)-8)*4)
        if(len(self.pdata) != (NWPHR*(1+NFAST)-8)*4):
            raise RuntimeError('ZEBRA physical packet data could not be read')

    def _read_ldata(self):
        ldata = b''
        NWLR = 0
        LRTYP = 0

        while(NWLR == 0):
            if(not self.pdata):
                self._read_pdata()
                if(not self.pdata):
                    return None,None,None     
        
            if(len(self.pdata) == 4):
                NWLR = struct.unpack('>I',self.pdata[0:4])[0]
                if(NWLR != 0):
                    raise RuntimeError('ZEBRA logical record size error:',NWLR)
                self.pdata = b''
                continue

            NWLR, LRTYP = struct.unpack('>II',self.pdata[0:8])
            if(NWLR == 0):
                self.pdata = self.pdata[4:]
                continue
            elif(NWLR*4 < len(self.pdata)-8):
                ldata = self.pdata[8:NWLR*4+8]
                self.pdata = self.pdata[NWLR*4+8:]
            else:
                ldata = self.pdata[8:]
                self.pdata = b''

        while(NWLR*4!=len(ldata)):
            if(not self.pdata):
                self._read_pdata()
                if(not self.pdata):
                    raise RuntimeError('ZEBRA file EOF with incomplete logical packet')

            if(self.NWTOLR == 0):
                ldata += self.pdata
                self.pdata = b''
                continue
            elif(self.NWTOLR>8):
                ldata += self.pdata[0:(self.NWTOLR-8)*4]
                self.pdata = self.pdata[(self.NWTOLR-8)*4:]
            else:
                raise RuntimeError('ZEBRA new logical packet while processing incomplete logical packet')

        return NWLR,LRTYP,ldata
    
    def _read_udata(self):
        LRTYP = 0
        while(LRTYP!=2 and LRTYP!=3):
            NWLR,LRTYP,ldata = self._read_ldata()
            if(not ldata):
                return None, None, None, None, None, None, None
            if(LRTYP==4):
                raise RuntimeError('ZEBRA logical extension found where start expected')
            if(self.verbose and LRTYP!=2 and LRTYP!=3):
                print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)")

        if(len(ldata)<40):
            raise RuntimeError('ZEBRA logical record too short for header')
        magic,QVERSIO,opt,zero,NWTX,NWSEG,NWTAB,NWBK,LENTRY,NWUHIO = struct.unpack('>IIIIIIIIII',ldata[0:40])
        if(magic!=0x4640e400):
            raise RuntimeError('ZEBRA logical record MAGIC not found')
        NWBKST = NWLR - (10 + NWUHIO + NWSEG + NWTX + NWTAB)

        if(self.verbose):
            print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}, NWTX={NWTX}, NWSEG={NWSEG}, NWTAB={NWTAB}, NWBK={NWBK}, LENTRY={LENTRY}, NWUHIO={NWUHIO},  NWBKST={NWBKST}, len={len(ldata)}")

        while(NWBKST<NWBK):
            NWLR,LRTYP,xldata = self._read_ldata()
            if(not ldata):
                raise RuntimeError('ZEBRA end of file while searching for logical extension')
            if(LRTYP==2 or LRTYP==3):
                raise RuntimeError('ZEBRA logical start found where extension expected')
            if(LRTYP==4):
                ldata += xldata
                NWBKST += NWLR
                if(self.verbose):
                    print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}")
            elif(self.verbose):
                print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)")

        if(NWBKST != NWBK):
            raise RuntimeError('ZEBRA number of bank words found does not match expected')

        if(NWUHIO!=0):
            if(len(ldata)<44):
                raise RuntimeError('ZEBRA logical record does not have user header IO control words')
            UHIOCW, = struct.unpack('>I',ldata[40:44])
            NWIO = self._nio(UHIOCW)
        else:
            UHIOCW=0
            NWIO=0
        NWUH = NWUHIO-NWIO
        NWBKST = NWLR - (10 + NWIO + NWUH + NWSEG + NWTX + NWTAB)

        return NWTX, NWSEG, NWTAB, NWBK, LENTRY, NWUH, ldata[(10+NWIO)*4:]

    def read(self):
        NWTX, NWSEG, NWTAB, NWBK, LENTRY, NWUH, udata = self._read_udata()

        if(not udata):
            return None

        runno = 0
        eventno = 0

        DSS = 0

        if(NWUH>0):
            if(self.verbose):
                print(f"UH:",end="")
                for i in range(NWUH):
                    x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                    if(i):
                        print(",",end="")
                    print(f" UH({i})={x}",end="")
                print()
            if(NWUH==2):
                runno, eventno = struct.unpack('>II',udata[DSS:DSS+8])
        DSS += NWUH

        if(self.verbose and NWSEG>0):
            print(f"ST:",end="")
            for i in range(NWSEG):
                x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                if(i):
                    print(",",end="")
                print(f" ST({i})={x}",end="")
            print()
        DSS += NWSEG

        if(self.verbose and NWTX>0):
            print(f"TV:",end="")
            for i in range(NWTX):
                x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                if(i):
                    print(",",end="")
                print(f" TV({i})={x}",end="")
            print()
        DSS += NWTX

        if(self.verbose and NWTAB>0):
            print(f"RT:",end="")
            for i in range(NWTAB):
                x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                if(i):
                    print(",",end="")
                print(f" RT({i})={x}",end="")
            print()
        DSS += NWTAB

        IOCB, = struct.unpack('>I',udata[DSS*4:(DSS+1)*4])
        NIO = self._nio(IOCB)
        DSS += 1+NIO

        NXTPTR,UPPTR,ORIGPTR,NBID,HBID,NLINK,NSTRUCLINK,NDW,STATUS = struct.unpack('>IIIIIIIII',udata[DSS*4:(DSS+9)*4])
        DSS += 9

        if(self.verbose):
            HBID_str = struct.pack('I',HBID).decode("utf-8")
            print(f"BH: IOCB={IOCB}, NXTPTR={NXTPTR}, UPPTR={UPPTR}, ORIGPTR={ORIGPTR}, NBID={NBID}, HBID={HBID} ({HBID_str}), NLINK={NLINK}, NSTRUCLINK={NSTRUCLINK}, NDW={NDW}, STATUS={STATUS}, len={len(udata)}")

        if(HBID == 0x45545445): # ETTE - 10m event bank
            return self._decode_ette(NDW, udata[DSS*4:])
        
        return None

    def _decode_ette(self, NDW, data):
        for i in range(min(NDW,1000)):
            x,  = struct.unpack('>I',data[i*4:(i+1)*4])
            if(i%8==0):
                print(f'{i*4:4d} |',end='')
            print(f"  {x:10d}",end='')
            if(i%8==7):
                print()
        if(NDW%8!=7):
            print()

        gdf_version, = struct.unpack('>I',data[0:4])
        if(gdf_version != 83):
            raise RuntimeError(f'Only GDF version 83 is supported (this file is version {gdf_version})')

        nadc, run_num, event_num, livetime_sec, livetime_ns = struct.unpack('>5I',data[28:48])
        elaptime_sec, elaptime_ns, grs_day, grs_time, grs_time_10ns = struct.unpack('>5I',data[84:104])
        trigger, = struct.unpack('>I',data[112:116])

        utc_time = (float(((grs_time&0x00F00000) >> 20)*60*60*10 +
	                     ((grs_time&0x000F0000) >> 16)*60*60 +
	                     ((grs_time&0x0000F000) >> 12)*60*10 +
	                     ((grs_time&0x00000F00) >> 8)*60 +
	                     ((grs_time&0x000000F0) >> 4)*10 +
	                     ((grs_time&0x0000000F) >> 0)) +
                   float(grs_time_10ns)/100000000.0)

        ev = dict(
            packet_type     = 'event',
            nadc            = nadc,
            run_num         = run_num, 
            event_num       = event_num, 
            livetime_sec    = livetime_sec, 
            livetime_ns     = livetime_ns,
            elaptime_sec    = elaptime_sec,
            elaptime_ns     = elaptime_ns,
            grs_data        = [ grs_day, grs_time, grs_time_10ns ],
            mjd_date        = grs_day,
            utc_time_sec    = utc_time,
            utc_time_str    = f'{grs_time:06x}',
            event_type      = 'pedestal' if trigger==1 else 'physics'
        )
        print(ev)
        return ev
    
