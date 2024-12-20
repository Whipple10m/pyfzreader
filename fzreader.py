# fzreader.py - Stephen Fegan - 2024-11-08
#
# Read Whipple 10m data in GDF/ZEBRA format into Python
#
# The Granite data format (GDF) uses the CERN ZEBRA package to store Whipple
# events in data banks. ZEBRA consists of three layers : physical, logical and
# data bank, each of which have headers that must be decoded. The ZEBRA format
# is described by "Overview of the ZEBRA System" (CERN Program Library Long 
# Writeups Q100/Q101), and in particular Chapter 10 describes the layout of the
# headers and data in "exchange mode".
# 
# https://cds.cern.ch/record/2296399/files/zebra.pdf
#
# Inside the data banks, the GDF code, written by Joachim Rose at Leeds,
# directs the writing of the individual data elements in blocks of data all of
# whom have the same data type (blocks of I32, blocks of I16 etc.). See for 
# example the function GDF$EVENT10 and observe the calls to GDF$MOVE
#
# https://github.com/Whipple10m/GDF/blob/main/gdf.for
#
# Copyright 2024, Stephen Fegan <sfegan@llr.in2p3.fr>
# Laboratoire Leprince-Ringuet, CNRS/IN2P3, Ecole Polytechnique, Institut Polytechnique de Paris
#
# This file is part of "pyfzreader"
#
# "pyfzreader" is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License version 2 or later, as published by
# the Free Software Foundation.
#
# "pyfzreader" is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

import struct
import time
import sys
import bz2
import gzip
import subprocess

# Can you modify this to automatically handle data in BZ2, gzip and UNIX compress format

class FZReader:
    def __init__(self, filename, verbose=False, verbose_file=None) -> None:
        self.filename = filename
        if(not filename):
            raise RuntimeError('No filename given: ' + filename)
        self.file = None
        self.saved_pdata = b''
        self.verbose = verbose
        self.verbose_file = verbose_file
        self.vstream = sys.stdout
        pass

    def __enter__(self):
        if self.filename.endswith('.bz2'):
            self.file = bz2.open(self.filename, 'rb')
        elif self.filename.endswith('.gz') or self.filename.endswith('.fzg'):
            self.file = gzip.open(self.filename, 'rb')
        elif self.filename.endswith('.Z') or self.filename.endswith('.fzz'):
            # Use gunzip rather than uncompress as latter insists filename end with ".Z"
            self.file = subprocess.Popen(['gunzip', '-c', self.filename], stdout=subprocess.PIPE).stdout
        else:
            self.file = open(self.filename, 'rb')
        self.saved_pdata = b''
        self.vstream = open(self.verbose_file, 'w') if self.verbose_file else sys.stdout
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()
        if self.vstream is not sys.stdout:
            self.vstream.close()
        self.vstream = sys.stdout

    def __iter__(self):
        return self

    def __next__(self):
        record = self.read()
        if not record:
            raise StopIteration
        return record

    def _nio(self, iocb):
        if(iocb < 12):
            return 1;
        else:
            return iocb&0xFFFF - 12;

    def _read_pdata(self):
        pdata = self.file.read(4*4)
        if(len(pdata) == 0):
            return None, None # EOF
        if(len(pdata) != 16):
            raise RuntimeError('ZEBRA physical record MAGIC could not be read')
        if(struct.unpack('>IIII',pdata) != (0x0123CDEF,0x80708070,0x4321ABCD,0x80618061)):
            raise RuntimeError('ZEBRA physical record MAGIC not found')

        pdata = self.file.read(4*4)
        if(len(pdata) != 16):
            raise RuntimeError('ZEBRA physical record header could not be read')
        NWPHR, PRC, NWTOLR, NFAST = struct.unpack('>IIII',pdata)
        NWPHR = NWPHR & 0xFFFFFF
        if(self.verbose):
            print(f"PH: NWPHR={NWPHR}, PRC={PRC}, NWTOLR=NWTOLR, NFAST={NFAST}",file=self.vstream)

        pdata = self.file.read((NWPHR*(1+NFAST)-8)*4)
        if(len(pdata) != (NWPHR*(1+NFAST)-8)*4):
            raise RuntimeError('ZEBRA physical packet data could not be read')

        return NWTOLR, pdata

    def _read_ldata(self):
        ldata = b''
        NWLR = 0
        LRTYP = 0
        while(NWLR == 0):
            if(self.saved_pdata):
                pdata = self.saved_pdata
                self.saved_pdata = b''
            else:
                NWTOLR, pdata = self._read_pdata()
                if(not pdata):
                    return None,None,None
                if(NWTOLR != 8):
                    raise RuntimeError('ZEBRA physical packet has unexpected data before logical record')
        
            if(len(pdata) == 4):
                NWLR = struct.unpack('>I',pdata[0:4])[0]
                if(NWLR != 0):
                    raise RuntimeError('ZEBRA logical record size error:',NWLR)
                pdata = b''
                continue

            NWLR, LRTYP = struct.unpack('>II',pdata[0:8])
            if(NWLR == 0):
                pdata = pdata[4:]
                continue
            elif(LRTYP == 5 or LRTYP == 6):
                # Skip padding records - assume these are only at end of PR
                NWLR = 0
            elif(NWLR*4 < len(pdata)-8):
                ldata = pdata[8:NWLR*4+8]
                self.saved_pdata = pdata[NWLR*4+8:]
            else:
                ldata = pdata[8:]

        while(NWLR*4>len(ldata)):
            NWTOLR, pdata = self._read_pdata()
            if(not pdata):
                raise EOFError('ZEBRA file EOF with incomplete logical packet')

            if(NWTOLR == 0):
                ldata += pdata
                continue
            elif(NWTOLR>8):
                ldata += pdata[0:(NWTOLR-8)*4]
                self.saved_pdata = pdata[(NWTOLR-8)*4:]
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
                print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)",file=self.vstream)

        if(len(ldata)<40):
            raise RuntimeError('ZEBRA logical record too short for header')
        magic,QVERSIO,opt,zero,NWTX,NWSEG,NWTAB,NWBK,LENTRY,NWUHIO = struct.unpack('>IIIIIIIIII',ldata[0:40])
        if(magic!=0x4640e400):
            raise RuntimeError('ZEBRA logical record MAGIC not found')
        NWBKST = NWLR - (10 + NWUHIO + NWSEG + NWTX + NWTAB)

        if(self.verbose):
            print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}, NWTX={NWTX}, NWSEG={NWSEG}, NWTAB={NWTAB}, NWBK={NWBK}, LENTRY={LENTRY}, NWUHIO={NWUHIO},  NWBKST={NWBKST}, len={len(ldata)}",file=self.vstream)

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
                    print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}",file=self.vstream)
            elif(self.verbose):
                print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)",file=self.vstream)

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
        if(self.verbose):
            print('-'*80,file=self.vstream)
            print(f'Read called: len(saved_pdata)={len(self.saved_pdata)}',file=self.vstream)

        NWTX, NWSEG, NWTAB, NWBK, LENTRY, NWUH, udata = self._read_udata()

        if(not udata):
            return None

        runno = 0
        eventno = 0

        DSS = 0

        if(NWUH>0):
            if(self.verbose):
                print(f"UH:",end="",file=self.vstream)
                for i in range(NWUH):
                    x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                    if(i):
                        print(",",end="",file=self.vstream)
                    print(f" UH({i})={x}",end="",file=self.vstream)
                print(file=self.vstream)
            if(NWUH==2):
                runno, eventno = struct.unpack('>II',udata[DSS:DSS+8])
        DSS += NWUH

        if(self.verbose and NWSEG>0):
            print(f"ST:",end="",file=self.vstream)
            for i in range(NWSEG):
                x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                if(i):
                    print(",",end="",file=self.vstream)
                print(f" ST({i})={x}",end="",file=self.vstream)
            print(file=self.vstream)
        DSS += NWSEG

        if(self.verbose and NWTX>0):
            print(f"TV:",end="",file=self.vstream)
            for i in range(NWTX):
                x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                if(i):
                    print(",",end="",file=self.vstream)
                print(f" TV({i})={x}",end="",file=self.vstream)
            print(file=self.vstream)
        DSS += NWTX

        if(self.verbose and NWTAB>0):
            print(f"RT:",end="",file=self.vstream)
            for i in range(NWTAB):
                x, = struct.unpack('>I',udata[(DSS+i)*4:(DSS+i+1)*4])
                if(i):
                    print(",",end="",file=self.vstream)
                print(f" RT({i})={x}",end="",file=self.vstream)
            print(file=self.vstream)
        DSS += NWTAB

        IOCB, = struct.unpack('>I',udata[DSS*4:(DSS+1)*4])
        NIO = self._nio(IOCB)
        DSS += 1+NIO

        NXTPTR,UPPTR,ORIGPTR,NBID,HBID,NLINK,NSTRUCLINK,NDW,STATUS = struct.unpack('>IIIIIIIII',udata[DSS*4:(DSS+9)*4])
        DSS += 9

        if(self.verbose):
            HBID_str = struct.pack('I',HBID).decode("utf-8")
            print(f"BH: IOCB={IOCB}, NXTPTR={NXTPTR}, UPPTR={UPPTR}, ORIGPTR={ORIGPTR}, NBID={NBID}, HBID={HBID} ({HBID_str}), NLINK={NLINK}, NSTRUCLINK={NSTRUCLINK}, NDW={NDW}, STATUS={STATUS}, len={len(udata)}",file=self.vstream)

        if(self.verbose=='max'):
            self._print_record(NDW, udata[DSS*4:])

        if(HBID == 0x45545445): # ETTE - 10m event
            return self._decode_ette(NDW, udata[DSS*4:])
        elif(HBID == 0x52555552): # RUUR - Run header
            return self._decode_ruur(NDW, udata[DSS*4:])
        elif(HBID == 0x48565648): # HVVH - High voltage settings
            return self._decode_hvvh(NDW, udata[DSS*4:])
        elif(HBID == 0x46545446): # FTTF - 10m frame
            return self._decode_fttf(NDW, udata[DSS*4:])
        elif(HBID == 0x54525254): # TRRT - Tracking information
            return self._decode_trrt(NDW, udata[DSS*4:])

        return dict(record_type     = 'unknown',
                    bank_id         = struct.pack('I',HBID).decode("utf-8"))

    def _print_record(self, NDW, data):
        for i in range(min(NDW,1000)):
            x,  = struct.unpack('>I',data[i*4:(i+1)*4])
            if(i%8==0):
                print(f'{i*4:4d} |',end='',file=self.vstream)
            print(f"  {x:10d}",end='',file=self.vstream)
            if(i%8==7):
                print(file=self.vstream)
        if(NDW%8!=7):
            print(file=self.vstream)
        return

    def _unpack_block(self, NFIRST, NDW, data, datum_code, datum_len):
        if(NFIRST >= NDW):
            raise RuntimeError(f'GDF bank data does not have block header: {NFIRST} >= {NDW}')
        block_header, = struct.unpack('>I',data[NFIRST*4:(NFIRST+1)*4])
        NW = block_header>>4
        if(NFIRST+NW >= NDW):
            raise RuntimeError(f'GDF bank data does not have full block: {NFIRST+NW} >= {NDW}')
        FMT = f'>{NW*4//datum_len}{datum_code}'
        block_values = struct.unpack(FMT,data[(NFIRST+1)*4:(NFIRST+1+NW)*4])
        if(self.verbose=='max' or self.verbose=='bank'):
            print(f"BBH: NW={NW}",block_values,file=self.vstream)
        elif(self.verbose):
            print(f"BBH: NW={NW}",file=self.vstream)
        return NW+1, block_values
    
    def _unpack_block_I32(self, NFIRST, NDW, data):
        return self._unpack_block(NFIRST, NDW, data, 'I', 4)

    def _unpack_block_I16(self, NFIRST, NDW, data):
        return self._unpack_block(NFIRST, NDW, data, 'H', 2)

    def _unpack_block_F32(self, NFIRST, NDW, data):
        return self._unpack_block(NFIRST, NDW, data, 'f', 4)

    def _unpack_block_F64(self, NFIRST, NDW, data):
        return self._unpack_block(NFIRST, NDW, data, 'd', 8)

    def _unpack_block_S(self, NFIRST, NDW, data):
        return self._unpack_block(NFIRST, NDW, data, 's', 1)

    def _unpack_gdf_header(self, data, record_type):
        gdf_version, = struct.unpack('>I',data[0:4])
        record_time_mjd, = struct.unpack('>d',data[16:24])
        NW = 6 if gdf_version>=27 else 5 # It's 7/6 in the GDF FORTRAN code.. but they start from 1
        record = dict(
            record_type         = record_type,
            record_time_mjd     = self._mjd_cleaned(record_time_mjd),
            record_time_str     = self._mjd_to_utc_string(record_time_mjd),
            record_was_decoded  = False,
            gdf_version         = gdf_version)
        return NW, record
    
    def _bytes_to_string(self, bytes_string):
        return ''.join(chr(b) for b in bytes_string if (32 <= b <= 126) or b in (9, 10, 13))

    def _mjd_cleaned(self, mjd):
        if(mjd!=mjd or mjd>55927 or mjd<48622.0):
            # MJD is NaN or out of range
            return 0
        return mjd

    def _mjd_to_utc_string(self, mjd):
        if(self._mjd_cleaned(mjd)==0):
            # MJD is NaN or out of range
            return 'unknown'
        epoch_time = max(round((mjd-40587.0)*86400000)*0.001,0)
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(epoch_time)))+f'.{int(epoch_time*1000)%1000:03d}'

    def _decode_ette(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'event')
        if(record['gdf_version'] < 74):
           return record
           
        NW, block_values = self._unpack_block_I32(NFIRST, NDW, data)
        NFIRST += NW
        nadc, run_num, event_num, livetime_sec, livetime_ns = block_values[0:5]
        ntrigger, elaptime_sec, elaptime_ns = block_values[13:16]
        grs_time_10MHz, grs_time, grs_day = block_values[16:19]

        NW, block_values = self._unpack_block_I32(NFIRST, NDW, data)
        NFIRST += NW
        trigger = block_values[0]

        trigger_data = ()
        if(ntrigger>0):
            NW, trigger_data = self._unpack_block_I32(NFIRST, NDW, data)
            NFIRST += NW

        adc_values = ()
        if(nadc>0):
            NW, adc_values = self._unpack_block_I16(NFIRST, NDW, data)
            NFIRST += NW

        grs_utc_isec = ((grs_time&0x00F00000) >> 20)*36000 + \
                       ((grs_time&0x000F0000) >> 16)*3600 + \
                       ((grs_time&0x0000F000) >> 12)*600 + \
                       ((grs_time&0x00000F00) >> 8)*60 + \
                       ((grs_time&0x000000F0) >> 4)*10 + \
                       ((grs_time&0x0000000F) >> 0)
        
        grs_doy = ((grs_day&0x00000F00) >> 8)*100 + \
                  ((grs_day&0x000000F0) >> 4)*10 + \
                  ((grs_day&0x0000000F) >> 0)

        grs_utc_time_sec = float(grs_utc_isec) + float(grs_time_10MHz)*1e-7

        grs_utc_time_str = f'{(grs_time>>16)&0xFF:02x}:{(grs_time>>8)&0xFF:02x}:{grs_time&0xFF:02x}.{grs_time_10MHz:07d}'
        
        record.update(dict(
            record_was_decoded  = True,
            run_num             = run_num, 
            event_num           = event_num, 
            livetime_sec        = livetime_sec, 
            livetime_ns         = livetime_ns,
            elaptime_sec        = elaptime_sec,
            elaptime_ns         = elaptime_ns,
            grs_data            = [ grs_time_10MHz, grs_time, grs_day ],
            grs_doy             = grs_doy,
            grs_utc_time_sec    = grs_utc_time_sec,
            grs_utc_time_str    = grs_utc_time_str,
            event_type          = 'pedestal' if trigger==1 else 'sky',
            nadc                = nadc,
            ntrigger            = ntrigger,
            trigger_data        = trigger_data,
            adc_values          = adc_values
        ))
        return record

    def _decode_ruur(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'run')

        NW, block_values = self._unpack_block_I32(NFIRST, NDW, data) # unused
        NFIRST += NW

        NW, block_values = self._unpack_block_I32(NFIRST, NDW, data)
        NFIRST += NW
        run_num = block_values[3]
        sky_quality = block_values[5]
        trig_mode = block_values[6]
        comment_len = block_values[12]

        NW, block_values = self._unpack_block_F32(NFIRST, NDW, data)
        NFIRST += NW
        sid_length = block_values[0]

        NW, block_values = self._unpack_block_F64(NFIRST, NDW, data)
        NFIRST += NW
        nominal_mjd_start, nominal_mjd_end = block_values

        if(record['gdf_version'] >= 27):
            NW, block_values = self._unpack_block_S(NFIRST, NDW, data)
            NFIRST += NW
            observers = self._bytes_to_string(block_values[0][80:])

            NW, block_values = self._unpack_block_S(NFIRST, NDW, data)
            NFIRST += NW
            comment = self._bytes_to_string(block_values[0])
        else:
            NFIRST += 1
            # Filename would be here but it doesn't seem to be used
            NFIRST += 20
            observers = self._bytes_to_string(data[NFIRST*4:(NFIRST+20)*4])
            NFIRST += 20
            comment = self._bytes_to_string(data[NFIRST*4:(NFIRST+comment_len)*4])

        record.update(dict(
            record_was_decoded  = True,
            run_num             = run_num, 
            sky_quality         = chr(64+sky_quality) if (sky_quality>0 and sky_quality<3) else '?',
            trig_mode           = trig_mode,
            sid_length          = sid_length,
            nominal_mjd_start   = nominal_mjd_start,
            nominal_mjd_end     = nominal_mjd_end,
            observers           = observers.strip(),
            comment             = comment.strip()
        ))
        return record

    def _decode_hvvh(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'hv')
        if(record['gdf_version'] < 67):
           return record

        NW, block_values = self._unpack_block_I32(NFIRST, NDW, data)
        NFIRST += NW
        _, mode_code, num_channels, read_cycle = block_values

        status = ()
        v_set = ()
        v_actual = ()
        i_supply = ()
        i_anode = ()
        if(num_channels > 0):
            NW, status = self._unpack_block_I16(NFIRST, NDW, data)
            NFIRST += NW

            NW, v_set = self._unpack_block_F32(NFIRST, NDW, data)
            NFIRST += NW

            NW, v_actual = self._unpack_block_F32(NFIRST, NDW, data)
            NFIRST += NW

            NW, i_supply = self._unpack_block_F32(NFIRST, NDW, data)
            NFIRST += NW

            NW, i_anode = self._unpack_block_F32(NFIRST, NDW, data)
            NFIRST += NW

        record.update(dict(
            record_was_decoded  = True,
            mode_code           = mode_code,
            num_channels        = num_channels,
            read_cycle          = read_cycle,
            status              = status,
            v_set               = v_set,
            v_actual            = v_actual,
            i_supply            = i_supply,
            i_anode             = i_anode
        ))
        return record

    def _decode_fttf(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'frame')
        # Ignore 10m frames for the moment, they weren't used in the 490 pixel camera
        return record

    def _hms_string(self, angle_rad):
        TENTHSEC = 10*3600.0*12.0/3.14159265358979324
        x = int(round(angle_rad * TENTHSEC))
        return f'{x//36000:02d}h{(x//600)%60:02d}m{(x%600)/10.0:04.1f}s'

    def _dms_string(self, angle_rad):
        TENTHSEC = 10*3600.0*180.0/3.14159265358979324
        x = int(round(abs(angle_rad) * TENTHSEC))
        return f'{"+" if angle_rad>=0 else "-"}{x//36000:03d}d{(x//600)%60:02d}m{(x%600)/10.0:04.1f}s'

    def _decode_trrt(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'tracking')
        if(record['gdf_version'] < 80):
           return record

        NW, block_values = self._unpack_block_I32(NFIRST, NDW, data) # unused
        NFIRST += NW
        mode, read_cycle = block_values[1:3]

        NW, block_values = self._unpack_block_I32(NFIRST, NDW, data)
        NFIRST += NW
        status = block_values[0]

        NW, block_values = self._unpack_block_F64(NFIRST, NDW, data)
        NFIRST += NW
        target_ra, target_dec = block_values[2:4]
        telescope_az, telescope_el, tracking_error = block_values[6:9]
        onoff_offset_ra, onoff_offset_dec, sidereal_time = block_values[9:12]

        NW, block_values = self._unpack_block_S(NFIRST, NDW, data)
        NFIRST += NW
        target = self._bytes_to_string(block_values[0])

        DEG = 180.0/3.14159265358979324
        HRS = 12.0/3.14159265358979324

        mode_name = {1:'on', 2:'off', 3:'slewing', 4:'standby',
                     5:'zenith', 6:'check', 7:'stowing', 8:'drift'}

        record.update(dict(
            record_was_decoded          = True,
            mode                        = mode_name.get(mode,'unknown'),
            mode_code                   = mode,
            read_cycle                  = read_cycle,
            status                      = status,
            target_ra_hours             = target_ra * HRS,
            target_ra_hms_str           = self._hms_string(target_ra),
            target_dec_deg              = target_dec * DEG,
            target_dec_dms_str          = self._dms_string(target_dec),
            telescope_az_deg            = telescope_az * DEG,
            telescope_el_deg            = telescope_el * DEG,
            tracking_error_deg          = tracking_error * DEG,
            onoff_offset_ra_hours       = onoff_offset_ra * HRS,
            onoff_offset_ra_hms_str     = self._hms_string(onoff_offset_ra),
            onoff_offset_dec_deg        = onoff_offset_dec * DEG,
            onoff_offset_dec_dms_str    = self._dms_string(onoff_offset_dec),
            sidereal_time_hours         = sidereal_time * HRS,
            sidereal_time_hms_str       = self._hms_string(sidereal_time),
            target                      = target.strip()    
        ))
        return record

if __name__ == '__main__':
    import json
    import getopt
    import sys

    input_file = None
    output_file = None  # Default to stdout
    verbose = False
    verbose_file = None

    args = sys.argv[1:]
    try: 
        opts, args = getopt.getopt(args, "vo:d:", ["output=", "debug="])
    except getopt.GetoptError as e:
        print(str(e), file=sys.stderr)
        print("Usage: fzreader.py [-o <output_file> | --output=<output_file>] [-d <debug_file> | --debug=<debug_file>] <input_file.fz>", file=sys.stderr)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-o", "--output"):
            output_file = arg
        if opt in ("-v"):
            if verbose == 'bank': verbose = 'max'
            if verbose == True: verbose = 'bank'
            if verbose == False: verbose = True
        if opt in ("-d", "--debug"):
            verbose_file = arg

    if len(args) < 1:
        print("Error: An input file must be specified.", file=sys.stderr)
        print("Usage: fzreader.py [-o <output_file> | --output=<output_file>] [-d <debug_file> | --debug=<debug_file>] <input_file.fz>", file=sys.stderr)
        sys.exit(1)
    else:
        input_file = args[0]

    with FZReader(input_file, verbose=verbose, verbose_file=verbose_file) as reader:
        with open(output_file, 'w') if output_file else sys.stdout as output:
            output.write('[')            
            i = 0
            record = reader.read()
            while(record):
                if(i):
                    output.write(',\n ')
                else:
                    output.write('\n ')
                json.dump(record, fp=output)
                i+=1
                record = reader.read()
            output.write('\n]\n')
