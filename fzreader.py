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
    
    def read(self):
        LRTYP = 1
        while(LRTYP!=2 and LRTYP!=3):
            NWLR,LRTYP,ldata = self._read_ldata()
            if(LRTYP==4):
                raise RuntimeError('ZEBRA logical packet type 4 not supoprted, please contact Stephen Fegan')
            if(self.verbose and LRTYP!=2 and LRTYP!=3):
                print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)")

        if(len(ldata)<40):
            raise RuntimeError('ZEBRA logical record too short for header')
        magic,QVERSIO,opt,zero,NWTX,NWSEG,NWTAB,NWBK,LENTRY,NWUHIO = struct.unpack('>IIIIIIIIII',ldata[0:40])
        if(magic!=0x4640e400):
            raise RuntimeError('ZEBRA logical record MAGIC not found')

        if(NWUHIO!=0):
            if(len(ldata)<44):
                raise RuntimeError('ZEBRA logical record does not have NWIO')
            NWIO, = struct.unpack('>I',ldata[40:44])
        else:
            NWIO=0
        NWUH = NWUHIO-NWIO
        NWBKST = NWLR - (10 + NWIO + NWUH + NWSEG + NWTX + NWTAB)
        if(self.verbose):
            print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}, NWTX={NWTX}, NWSEG={NWSEG}, NWTAB={NWTAB}, NWBK={NWBK}, LENTRY={LENTRY}, NWUHIO={NWUHIO}, NWUH={NWUH}, NWIO={NWIO}, NWBKST={NWBKST}")

        return NWLR,LRTYP,ldata
