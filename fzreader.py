# fzreader.py - Stephen Fegan - 2024-11-08

# The Granite data format (GDF) uses the CERN ZEBRA package to store Whipple
# events in data banks. ZEBRA consists of three layers: physical, logical, and
# data bank, each of which have headers that must be decoded. The ZEBRA format
# is described by "Overview of the ZEBRA System" (CERN Program Library Long 
# Writeups Q100/Q101), and in particular Chapter 10 describes the layout of the
# headers and data in "exchange mode".

# https://cds.cern.ch/record/2296399/files/zebra.pdf

# Inside the data banks, the GDF code, written by Joachim Rose at Leeds,
# directs the writing of the individual data elements in blocks of data all of
# whom have the same data type (blocks of I32, blocks of I16 etc.). See for 
# example the function GDF$EVENT10 and observe the calls to GDF$MOVE

# https://github.com/Whipple10m/GDF/blob/main/gdf.for

# This file is part of "pyfzreader"

# "pyfzreader" is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License version 2 or later, as published by
# the Free Software Foundation.

# "pyfzreader" is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

"""
Read Whipple 10m data in GDF/ZEBRA format into Python.
"""

import os
import struct
import time
import sys
import bz2
import gzip
import subprocess
import json

_camera_cache = None

def is_pedestal_event(record):
    """
    Check if the given record is a pedestal event.

    Args:
        record (dict): The record to check.

    Returns:
        bool: True if the record is a pedestal event, False otherwise.
    """
    return record['record_type'] in ('event','frame') and record['record_was_decoded'] \
        and record['event_type'] == 'pedestal'

def is_sky_event(record):
    """
    Check if the given record is a sky event.

    Args:
        record (dict): The record to check.

    Returns:
        bool: True if the record is a sky event, False otherwise.
    """
    return record['record_type']=='event' and record['event_type'] == 'sky'

def get_camera_geometry_by_nadc(n):
    """
    Get the camera configuration corresponding to the given number of ADC channels
    or pixels.

    Args:
        n (int): The number of ADC channels, or pixels. This is rounded up to the
            nearest multiple of 12 before looking up the camera configuration.
            This means that, e.g. n=331 will return the same camera configuration 
            as n=336. Must round up to 120, 156, 336, 492, or 384, see 
            `whipple_cams.json` for details.

    Returns:
        dict: The camera configuration corresponding to the given value of n.
    """
    global _camera_cache
    if _camera_cache is None:
        module_dir = os.path.dirname(__file__)
        json_path = os.path.join(module_dir, 'whipple_cams.json')
        with open(json_path, 'r') as f:
            _camera_cache = json.load(f)
    return _camera_cache.get(str((nadc+11)//12*12))

class FZDecodeError(Exception):
    """Exception raised when an error occurs while decoding a ZEBRA/GDF record."""
    pass

class EmergencyStop(Exception):
    """Exception raised when an emergency stop flag is encountered in the 
    ZEBRA physical record. Used internally by the FZReader class."""
    pass

class FZReader:
    """
    A class to read Whipple 10m data in GDF/ZEBRA format.

    The FZReader class provides functionality to read and decode 
    Whipple 10m data stored in GDF/ZEBRA format. It supports various 
    compressed file formats such as bzip2, gzip, and LZW, as well as 
    uncompressed files.

    Usage:
        The primary way to use the FZReader is through the `read` 
        method or by iterating over the FZReader object. The `read` 
        method returns the next record from the file, while the 
        iterator interface allows for easy iteration over all 
        records in the file.

        Example:
            with FZReader('data.fz.gz') as reader:
                for record in reader:
                    print(record)
        
        Alternatively, you can use the `read` method directly:
            with FZReader('data.fz') as reader:
                record = reader.read()
                while record:
                    print(record)
                    record = reader.read()
    
    Recognized GDF Records:
        - Event data: including an array of ADC values, event number, type, 
            timestamps, and trigger code.
        - Frame data: supported for older GDF files where the pedestal and
            calibration data were separated from the event data. Returns the
            ADC data, frame number, and timestamps.
        - Run headers: including the run number, run start and stop times,
            and various comments entered by the observers.
        - Tracking information: including the tracking mode, name and 
            coordinates of the target (RA and Dec), position of the telescope
            in the sky (Az, El), and timestamp.
        - High voltage settings: including the high voltage settings and 
            measurements for each of the channels.
        - CCD information: recognized but not decoded.

        See the README.md file for details of the fields returned for each
        of the GDF records supported.

        https://github.com/Whipple10m/pyfzreader/blob/main/README.md

    Methods:
        read(): Read the next record from the file.

    Attributes:
        filename (str): The name of the FZ file to read. This can be 
            bzip2 (.bz2), gzip (.gz or .fzg), LZW (.Z or .fzz), or 
            uncompressed (any other extension).
        verbose (bool): If True, print verbose output, primarily for 
            diagnosing the decoding of the ZEBRA/GDF data elements.
        verbose_file (str): The file to write verbose output to (default 
            is None, corresponding to stdout).
        resynchronise_header (bool): If True, resynchronise the header.
    """

    def __init__(self, filename, verbose=False, verbose_file=None, resynchronise_header = False) -> None:
        """
        Initialize the FZReader.

        Args:
            filename (str): The name of the FZ file to read. This can be 
                bzip2 (.bz2), gzip (.gz or .fzg), LZW (.Z or .fzz), or 
                uncompressed (any other extension).
            verbose (bool): If True, print verbose output, primarily for 
                diagnosing the decoding of the ZEBRA/GDF data elements.
            verbose_file (str): The file to write verbose output to (default 
                is None, corresponding to stdout).
            resynchronise_header (bool): If True, resynchronise the header.
        """
        self.filename = filename
        if(not filename):
            raise RuntimeError('No filename given')
        self.file = None
        self.saved_pdata = b''
        self.verbose = verbose
        self.verbose_file = verbose_file
        self.vstream = sys.stdout
        self.end_of_run = False
        self.resynchronise_header = resynchronise_header
        self.packet_headers_found = 0
        self.nbytes_read = 0
        self.ph_start_byte = 0
        pass

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        Returns:
            FZReader: The FZReader object.
        """
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
        self.packet_headers_found = 0
        self.nbytes_read = 0
        self.ph_start_byte = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context related to this object.

        Args:
            exc_type (type): The exception type.
            exc_val (Exception): The exception value.
            exc_tb (traceback): The traceback object.
        """
        if self.file:
            self.file.close()
        if self.vstream is not sys.stdout:
            self.vstream.close()
        self.vstream = sys.stdout
        self.file = None

    def __iter__(self):
        """
        Return the iterator object itself.

        Returns:
            FZReader: The FZReader object.
        """
        return self

    def __next__(self):
        """
        Return the next record from the file. 
        
        See the `read` method for details of the records returned and 
        exceptions raised.

        Returns:
            dict: The next record.

        Raises:
            StopIteration: If there are no more records.
        """
        record = self.read()
        if not record:
            raise StopIteration
        return record

    def num_bytes_read(self):
        """
        Get the number of bytes read from the file.
        """
        return self.nbytes_read

    def num_packets_found(self):
        """
        Get the number of packet headers found in the file.
        """
        return self.packet_headers_found
    
    def last_packet_header_start_byte(self):
        """
        Get the start byte of the last packet header found in the file.
        """
        return self.ph_start_byte

    def _decode_sequence(self, NHW, seq_name, DSS, NDW, data):
        ndecode = min(NHW, NDW-DSS)
        values = struct.unpack(f'>{ndecode}I',data[DSS*4:(DSS+ndecode)*4])
        if(self.verbose and NHW>0):
            hstr = f'{seq_name}:'
            for i in range(ndecode):
                hstr += f' {values[i]}'
            for i in range(ndecode, NHW):
                hstr += f' (missing)'
            print(hstr,file=self.vstream)
        if(ndecode != NHW):
            raise FZDecodeError(f'GDF user data not have full {seq_name} sequence: {DSS}+{NHW} > {NDW}. PH start byte: {self.ph_start_byte}.')
        return DSS+NHW, values

    def read(self):
        """
        Read the next record from the file.

        Returns:
            dict: The next record, or None if there are no more records.
                See the README.md file for details of what is returned
                for each of the GDF records supported.

        Raises:
            EOFError: If the GDF file is incomplete, i.e. if the file ends 
                abruptly while decoding a ZEBRA physical or logical
                record, or the end-of-file record is not found before the
                end of the data.

            FZDecodeError: If the ZEBRA physical record MAGIC is not found,
                or some other error occurs while decoding the ZEBRA physical
                or logical record. In this case the `resynchronise_header`
                option may allow the reader to continue reading the file,
                but the data may be corrupted.
        """

        if(not self.file):
            raise RuntimeError('File not open. Must call __enter__ first, or use context manager.')

        if(self.verbose):
            print('-'*80,file=self.vstream)
            print(f'Read called: len(saved_pdata)={len(self.saved_pdata)//4} words',file=self.vstream)

        while(True):
            try:
                NWTX, NWSEG, NWTAB, _, _, NWUH, udata = self._read_udata()
                break
            except EmergencyStop:
                if(self.verbose):
                    print(f"PH: Emergency stop flag encountered, physical packet discarded.",file=self.vstream)

        if(not udata):
            return None

        DSS = 0
        NDW = len(udata)//4
        if(len(udata) != NDW*4):
            raise FZDecodeError(f'ZEBRA user data is not multiple of wordsize: {len(udata)} != {NUW*4}. PH start byte: {self.ph_start_byte}.')

        DSS, user_header = self._decode_sequence(NWUH, 'UH', DSS, NDW, udata)
        recordno, runno = user_header

        DSS, _ = self._decode_sequence(NWSEG, 'ST', DSS, NDW, udata)

        DSS, _ = self._decode_sequence(NWTX, 'TV', DSS, NDW, udata)   

        DSS, _ = self._decode_sequence(NWTAB, 'RT', DSS, NDW, udata)

        DSS, iocb_values = self._decode_sequence(1, 'IOCBH', DSS, NDW, udata)
        IOCB = iocb_values[0]
        NIO = self._nio(IOCB)
        if(self.verbose):
            print(f"IOCBH: IOCB={IOCB}, NIO={NIO}",file=self.vstream)

        DSS, _ = self._decode_sequence(NIO, 'IOCBD', DSS, NDW, udata)

        DSS, bank_header = self._decode_sequence(9, 'BH (raw)', DSS, NDW, udata)
        NXTPTR,UPPTR,ORIGPTR,NBID,HBID,NLINK,NSTRUCLINK,NDW,STATUS = bank_header

        if(self.verbose):
            HBID_str = struct.pack('I',HBID).decode("utf-8")
            print(f"BH: IOCB={IOCB}, NXTPTR={NXTPTR}, UPPTR={UPPTR}, ORIGPTR={ORIGPTR}, NBID={NBID}, HBID={HBID} ({HBID_str}), NLINK={NLINK}, NSTRUCLINK={NSTRUCLINK}, NDW={NDW}, STATUS={STATUS}, len(udata)={len(udata)//4} words",file=self.vstream)

        if(self.verbose=='max'):
            self._print_record(udata[DSS*4:])

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
        elif(HBID == 0x43434343): # CCCC - CCD information
            return self._decode_cccc(NDW, udata[DSS*4:])

        return dict(record_type     = 'unknown',
                    bank_id         = struct.pack('I',HBID).decode("utf-8"))

    def _nio(self, iocb):
        if(iocb < 12):
            return 1;
        else:
            return iocb&0xFFFF - 12;

    def _read_pdata(self):
        self.ph_start_byte = self.nbytes_read
        
        # Read ZEBRA physical record
        ZEBRA_MAGIC = (0x0123CDEF,0x80708070,0x4321ABCD,0x80618061)
        pdata = b''
        nadjust = 0
        while(len(pdata) != 32):
            try:
                data = self.file.read(32-len(pdata))
            except Exception as e:
                raise EOFError(f'Read error. PH start byte: {self.ph_start_byte}.') from e
            self.nbytes_read += len(data)
            pdata += data
            if(len(pdata) == 0):
                return None, None # EOF
            if(len(pdata) != 32):
                raise EOFError(f'ZEBRA physical record MAGIC and header could not be read. PH start byte: {self.ph_start_byte}.')
            if(struct.unpack('>IIII',pdata[:16]) == ZEBRA_MAGIC):
                break
            if(self.resynchronise_header):
                pdata = pdata[1:]
                self.ph_start_byte += 1
                nadjust += 1
            else:
                failed_magic = [f'{x:08x}' for x in struct.unpack('>IIII',pdata[:16])]
                raise FZDecodeError(f'ZEBRA physical record MAGIC not found. Values were {failed_magic}. PH start byte: {self.ph_start_byte}.')

        if(self.verbose and nadjust>0):
            print(f"PH: *WARNING* Adjusted header by {nadjust} bytes",file=self.vstream)
        
        _, pheader = self._decode_sequence(4, 'PH (raw)', 4, 8, pdata)
        NWPHR, PRC, NWTOLR, NFAST = pheader
        FLAGS = NWPHR >> 24
        NWPHR = NWPHR & 0xFFFFFF

        if(self.verbose):
            print(f"PH: Found npacket={self.packet_headers_found} at byte {self.ph_start_byte}, word {self.ph_start_byte/4}",file=self.vstream)
            print(f"PH: NWPHR={NWPHR}, PRC={PRC}, NWTOLR={NWTOLR}, NFAST={NFAST}, FLAGS=0x{FLAGS:02x}",file=self.vstream)

        self.packet_headers_found += 1

        if(NWPHR < 90):
            raise FZDecodeError(f'ZEBRA physical record length error: NWPHR={NWPHR}. PH start byte: {self.ph_start_byte}.')

        try:    
            pdata = self.file.read((NWPHR*(1+NFAST)-8)*4)
        except Exception as e:
            raise EOFError(f'Read error. PH start byte: {self.ph_start_byte}.') from e
        self.nbytes_read += len(pdata)
        if(len(pdata) != (NWPHR*(1+NFAST)-8)*4):
            raise EOFError(f'ZEBRA physical packet data could not be read. PH start byte: {self.ph_start_byte}.')

        if(FLAGS & 0x80):
            self.saved_pdata = b''
            # Emergency stop flag, discard packet after reading
            raise EmergencyStop(f'ZEBRA physical record has emergency-stop flag set. PH start byte: {self.ph_start_byte}.')

        return NWTOLR, pdata

    def _read_ldata(self):
        # Read ZEBRA logical record, skipping padding records. Physical frames
        # are read as necessary to get a complete logical record. Unused physical
        # frames data saved for the next logical record.
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
                    raise FZDecodeError(f'ZEBRA physical packet has unexpected data before logical record. PH start byte: {self.ph_start_byte}.')
        
            if(len(pdata) == 4):
                NWLR = struct.unpack('>I',pdata[0:4])[0]
                if(NWLR != 0):
                    raise FZDecodeError(f'ZEBRA logical record size error: {NWLR}. PH start byte: {self.ph_start_byte}.')
                pdata = b''
                continue

            _, lh_data = self._decode_sequence(2, 'LH (raw type)', 0, len(pdata)//4, pdata)
            NWLR, LRTYP = lh_data

            if(LRTYP > 6):
                if(self.verbose):
                    print(f"LH(PARTIAL): NWLR={NWLR}, LRTYP={LRTYP}",file=self.vstream)
                raise FZDecodeError(f'ZEBRA logical record type error: LRTYP={LRTYP} > 6. PH start byte: {self.ph_start_byte}.')

            if(NWLR == 0):
                # Skip implicit padding records
                pdata = pdata[4:]
                continue
            elif(LRTYP == 5 or LRTYP == 6):
                # Skip padding records - assume these are only at end of PR
                if(self.verbose):
                    print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)",file=self.vstream)
                NWLR = 0
            elif(NWLR*4 < len(pdata)-8):
                # Physical record contains more data after this logical record
                ldata = pdata[8:NWLR*4+8]
                # Save the rest of the physical data for the next logical record
                self.saved_pdata = pdata[NWLR*4+8:]
            else:
                # Physical record contains no more data after this logical record
                ldata = pdata[8:]

        while(NWLR*4>len(ldata)):
            if(self.saved_pdata):
                if(self.verbose):
                    print(f"LH(PARTIAL): NWLR={NWLR}, LRTYP={LRTYP}, len(ldata)={len(ldata)//4} words, len(self.saved_pdata)={len(self.saved_pdata)//4} words",file=self.vstream)
                raise FZDecodeError(f'Logic error: already has saved pdata but about to load more. PH start byte: {self.ph_start_byte}.')
        
            NWTOLR, pdata = self._read_pdata()
            if(not pdata):
                if(self.verbose):
                    print(f"LH(PARTIAL): NWLR={NWLR}, LRTYP={LRTYP}, len(ldata)={len(ldata)//4} words",file=self.vstream)
                raise EOFError(f'ZEBRA file EOF with incomplete logical packet. PH start byte: {self.ph_start_byte}.')

            if(NWTOLR == 0):
                ldata += pdata
                continue
            elif(NWTOLR>8):
                ldata += pdata[0:(NWTOLR-8)*4]
                self.saved_pdata = pdata[(NWTOLR-8)*4:]
            else:
                if(self.verbose):
                    print(f"LH(PARTIAL): NWLR={NWLR}, LRTYP={LRTYP}, len={len(ldata)//4} words",file=self.vstream)
                raise FZDecodeError(f'ZEBRA new logical packet while processing incomplete logical packet. PH start byte: {self.ph_start_byte}.')

        return NWLR,LRTYP,ldata
    
    def _read_udata(self):
        # Read ZEBRA user data, combining logical (extension) records as 
        # necessary and processing start-of-run and end-of-run records.
        LRTYP = 0
        while(LRTYP!=2 and LRTYP!=3):
            NWLR,LRTYP,ldata = self._read_ldata()
            if(not ldata):
                if(not self.end_of_run):
                    raise EOFError(f'ZEBRA file end-of-file not found before end of data. PH start byte: {self.ph_start_byte}.')
                return None, None, None, None, None, None, None
            if(LRTYP == 1):
                # Start-of-run or end-of-run: flag the end-of-run for later use
                if(NWLR>0):
                    NRUN = struct.unpack('>i',ldata[0:4])[0]
                    if(self.verbose):
                        print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}, NRUN={NRUN} (skipping)",file=self.vstream)
                    if(NRUN<=0):
                        self.end_of_run = True
            elif(LRTYP==4):
                raise FZDecodeError(f'ZEBRA logical extension found where start expected. PH start byte: {self.ph_start_byte}.')
            elif(self.verbose and LRTYP!=2 and LRTYP!=3):
                print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)",file=self.vstream)

        DSS = 0
        try:
            DSS, lheader = self._decode_sequence(10, 'LH (raw)', DSS, NWLR, ldata)
        except Exception as e:
            raise FZDecodeError(f'ZEBRA logical record too short for header: len(ldata)={len(ldata)//4} words. PH start byte: {self.ph_start_byte}.') from e
        magic,_,_,_,NWTX,NWSEG,NWTAB,NWBK,LENTRY,NWUHIO = lheader
        if(magic!=0x4640e400):
            raise FZDecodeError(f'ZEBRA logical record MAGIC not found. PH start byte: {self.ph_start_byte}.')
        NWBKST = NWLR - (10 + NWUHIO + NWSEG + NWTX + NWTAB)

        if(self.verbose):
            print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}, NWTX={NWTX}, NWSEG={NWSEG}, NWTAB={NWTAB}, NWBK={NWBK}, LENTRY={LENTRY}, NWUHIO={NWUHIO},  NWBKST={NWBKST}, len(ldata)={len(ldata)//4} words",file=self.vstream)

        while(NWBKST<NWBK):
            NWLR,LRTYP,xldata = self._read_ldata()
            if(not xldata):
                raise FZDecodeError(f'ZEBRA end of file while searching for logical extension. PH start byte: {self.ph_start_byte}.')
            if(LRTYP==2 or LRTYP==3):
                raise FZDecodeError(f'ZEBRA logical start found where extension expected. PH start byte: {self.ph_start_byte}.')
            if(LRTYP==4):
                ldata += xldata
                NWBKST += NWLR
                if(self.verbose):
                    print(f"LH: NWLR={NWLR}, LRTYP={LRTYP}, NWBKST={NWBKST}",file=self.vstream)
            elif(self.verbose):
                print(f"LH: NWLR={NWLR}, LRTYP={LRTYP} (skipping)",file=self.vstream)

        if(NWBKST != NWBK):
            raise FZDecodeError(f'ZEBRA number of bank words found does not match expected: {NWBKST} != {NWBK}. PH start byte: {self.ph_start_byte}.')

        if(NWUHIO != 0):
            DSS, uhiocw_values = self._decode_sequence(1, 'UHIOCW', DSS, len(ldata)//4, ldata)
            UHIOCW = uhiocw_values[0]
            NWIO = self._nio(UHIOCW)
            if(self.verbose):
                print(f"UHIOCW: UHIOCW={UHIOCW}, NWIO={NWIO}",file=self.vstream)
        else:
            UHIOCW=0
            NWIO=0
        NWUH = NWUHIO-NWIO
        NWBKST = NWLR - (10 + NWIO + NWUH + NWSEG + NWTX + NWTAB)

        return NWTX, NWSEG, NWTAB, NWBK, LENTRY, NWUH, ldata[DSS*4:]

    def _print_record(self, data):
        nprint = min(len(data)//4, 1000)
        values = struct.unpack(f'>{nprint}I',data[:nprint*4])
        for i in range(nprint):
            if(i%8==0):
                print(f'{i*4:4d} |',end='',file=self.vstream)
            print(f"  {values[i]:10d}",end='',file=self.vstream)
            if(i%8==7):
                print(file=self.vstream)
        if(nprint and nprint%8!=0):
            print(file=self.vstream)
        if(len(data)//4 > nprint):
            print(f"  {nprint:10d} | ... continued ...",end='',file=self.vstream)
        return

    def _skip_block(self, NFIRST, NDW, data, nitems, datum_len):
        if(NFIRST+1 > NDW):
            raise FZDecodeError(f'GDF bank data does not have block header: {NFIRST}+1 > {NDW}. PH start byte: {self.ph_start_byte}.')
        block_header, = struct.unpack('>I',data[NFIRST*4:(NFIRST+1)*4])
        NFIRST += 1
        NW = block_header>>4
        if(NW != (nitems*datum_len + 3)//4):
            raise FZDecodeError(f'GDF bank data block size not as expected: {NW} != {nitems*datum_len//4}. PH start byte: {self.ph_start_byte}.')
        if(NFIRST+NW > NDW):
            raise FZDecodeError(f'GDF bank data does not have full block: {NFIRST}+{NW} > {NDW}. PH start byte: {self.ph_start_byte}.')
        NFIRST += NW
        return NFIRST

    def _unpack_block(self, NFIRST, NDW, data, nitems, datum_code, datum_len):
        if(NFIRST+1 > NDW):
            raise FZDecodeError(f'GDF bank data does not have block header: {NFIRST}+1 > {NDW}. PH start byte: {self.ph_start_byte}.')
        block_header, = struct.unpack('>I',data[NFIRST*4:(NFIRST+1)*4])
        NFIRST += 1
        NW = block_header>>4
        if(NW != (nitems*datum_len + 3)//4):
            raise FZDecodeError(f'GDF bank data block size not as expected: {NW} != {nitems*datum_len//4}. PH start byte: {self.ph_start_byte}.')
        if(NFIRST+NW > NDW):
            raise FZDecodeError(f'GDF bank data does not have full block: {NFIRST}+{NW} > {NDW}. PH start byte: {self.ph_start_byte}.')
        FMT = f'>{NW*4//datum_len}{datum_code}'
        block_values = struct.unpack(FMT,data[NFIRST*4:(NFIRST+NW)*4])
        NFIRST += NW
        if(self.verbose=='max' or self.verbose=='bank'):
            print(f"BBH: NW={NW}",block_values,file=self.vstream)
        elif(self.verbose):
            print(f"BBH: NW={NW}",file=self.vstream)
        return NFIRST, block_values
    
    def _unpack_block_I32(self, NFIRST, NDW, data, nitems):
        return self._unpack_block(NFIRST, NDW, data, nitems, 'I', 4)

    def _unpack_block_I16(self, NFIRST, NDW, data, nitems):
        return self._unpack_block(NFIRST, NDW, data, nitems, 'H', 2)

    def _unpack_block_F32(self, NFIRST, NDW, data, nitems):
        return self._unpack_block(NFIRST, NDW, data, nitems, 'f', 4)

    def _unpack_block_F64(self, NFIRST, NDW, data, nitems):
        return self._unpack_block(NFIRST, NDW, data, nitems, 'd', 8)

    def _unpack_block_S(self, NFIRST, NDW, data, nitems):
        return self._unpack_block(NFIRST, NDW, data, nitems, 's', 1)

    def _unpack_gdf_header(self, data, record_type):
        gdf_version, = struct.unpack('>I',data[0:4])
        NW=6 if gdf_version>=27 else 5 # 7/6 in FORTRAN but they start at 1
        record_time_mjd, = struct.unpack('>d',data[(NW-2)*4:NW*4])
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

    def _decode_truetime(self, grs_time_10MHz, grs_time, grs_day):
        grs_utc_isec = ((grs_time >> 20) & 0xF) * 36000 + \
                       ((grs_time >> 16) & 0xF) * 3600 + \
                       ((grs_time >> 12) & 0xF) * 600 + \
                       ((grs_time >>  8) & 0xF) * 60 + \
                       ((grs_time >>  4) & 0xF) * 10 + \
                       ((grs_time      ) & 0xF)
        
        grs_day_of_year = ((grs_day >> 8) & 0x3) * 100 + \
                          ((grs_day >> 4) & 0xF) * 10 + \
                          ((grs_day     ) & 0xF)

        grs_status = (grs_day >> 16) & 0xF

        grs_utc_time_sec = float(grs_utc_isec) + float(grs_time_10MHz)*1e-7

        grs_utc_time_str = f'{(grs_time>>16)&0xFF:02x}:{(grs_time>>8)&0xFF:02x}:{grs_time&0xFF:02x}.{grs_time_10MHz:07d}'

        return grs_day_of_year, grs_utc_time_sec, grs_utc_time_str, grs_status

    def _decode_gps(self, gps_low, gps_mid, gps_high):
        # Decode old Whipple GPS (See GPSTIME from fz2red)

        gps_day_of_year = ((gps_high >> 14) & 0x3) * 100 + \
                          ((gps_high >> 10) & 0xF) * 10 + \
                          ((gps_high >>  6) & 0xF)

        gps_us = ((gps_mid  & 0x3) <<  2) * 100000 + \
                 ((gps_low  >> 14) & 0x3) * 100000 + \
                 ((gps_low  >> 10) & 0xF) * 10000 + \
                 ((gps_low  >>  6) & 0xF) * 1000 + \
                 (gps_low & 0x3) * 250

        gps_status = (gps_low >> 2) & 0xF

        gps_utc_time_sec = ((gps_high >>  4) & 0x3) * 36000 + \
                           ((gps_high      ) & 0xF) * 3600 + \
                           ((gps_mid  >> 13) & 0x7) * 600 + \
                           ((gps_mid  >>  9) & 0xF) * 60 + \
                           ((gps_mid  >>  6) & 0x7) * 10 + \
                           ((gps_mid  >>  2) & 0xF) + gps_us*0.000001

        gps_utc_time_str = f'{gps_high&0x3F:02x}:{(gps_mid>>9)&0x7F:02x}:{(gps_mid>>2)&0x7F:02x}.{gps_us:05d}'

        return gps_day_of_year, gps_utc_time_sec, gps_utc_time_str, gps_status

    def _decode_ette(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'event')
        
        version_dependent_elements = dict()

        if(record['gdf_version'] >= 74):
            NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 20)
            nadc, run_num, event_num, livetime_sec, livetime_ns = block_values[0:5]
            ntrigger, elaptime_sec, elaptime_ns = block_values[13:16]
            grs_data_10MHz, grs_data_time, grs_data_day = block_values[16:19]

            NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 7)
            trigger_code = block_values[0]
            event_type = 'pedestal' if (trigger_code & 0x01) else 'sky'

            trigger_data = ()
            if(ntrigger>0):
                NFIRST, trigger_data = self._unpack_block_I32(NFIRST, NDW, data, ntrigger)

            adc_values = ()
            if(nadc>0):
                NFIRST, adc_values = self._unpack_block_I16(NFIRST, NDW, data, nadc)

            # Prefer to explicitly skip this block to test consistancy of data
            NFIRST = self._skip_block(NFIRST, NDW, data, 28, 2) 

            gps_truetime_grs = True
            gps_data = ( grs_data_10MHz, grs_data_time, grs_data_day )
            gps_day_of_year, gps_utc_time_sec, gps_utc_time_str, gps_status = self._decode_truetime(
                grs_data_10MHz, grs_data_time, grs_data_day)

            version_dependent_elements = dict(
                elaptime_sec        = elaptime_sec,
                elaptime_ns         = elaptime_ns,
                ntrigger            = ntrigger,
                trigger_data        = trigger_data,
            )
        else:
            NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 7)
            trigger_code = block_values[0]
            event_type = 'pedestal' if trigger_code==1 else 'sky'

            NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 13 if record['gdf_version'] >= 27 else 10)
            nadc, run_num, event_num, livetime_sec, livetime_ns = block_values[0:5]

            if(record['gdf_version'] >= 27):
                NFIRST, adc_values = self._unpack_block_I16(NFIRST, NDW, data, nadc)

                NFIRST, block_values = self._unpack_block_I16(NFIRST, NDW, data, 28)
                gps_data_mid, gps_data_high, _, gps_data_low = block_values[0:4]
            else:
                NFIRST, block_values = self._unpack_block_I16(NFIRST, NDW, data, 144)
                gps_data_mid, gps_data_high, _, gps_data_low = block_values[:4]
                adc_values = block_values[4:124]

            gps_truetime_grs = False
            gps_data = ( gps_data_low, gps_data_mid, gps_data_high )
            gps_day_of_year, gps_utc_time_sec, gps_utc_time_str, gps_status = self._decode_gps(
                gps_data_low, gps_data_mid, gps_data_high)

        record.update(dict(
            record_was_decoded  = True,
            run_num             = run_num, 
            event_num           = event_num, 
            livetime_sec        = livetime_sec, 
            livetime_ns         = livetime_ns,
            gps_truetime_grs    = gps_truetime_grs,
            gps_data            = gps_data,
            gps_day_of_year     = gps_day_of_year,
            gps_utc_time_sec    = gps_utc_time_sec,
            gps_utc_time_str    = gps_utc_time_str,
            gps_status          = gps_status,
            trigger_code        = trigger_code,
            event_type          = event_type,
            nadc                = nadc,
            adc_values          = adc_values
        ))

        record.update(version_dependent_elements)

        return record

    def _decode_fttf(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'frame')

        if(record['gdf_version'] < 74):
            # Only support frame data before version 74

            NFIRST = self._skip_block(NFIRST, NDW, data, 2, 4) # STATUS

            NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 8 if record['gdf_version'] >= 27 else 5)
            nphs, nadc, nsca, run_num, frame_num = block_values[0:5]

            if(record['gdf_version'] >= 27):
                NFIRST = self._skip_block(NFIRST, NDW, data, nadc, 2) # CAL_ADC unused
                NFIRST, adc_values = self._unpack_block_I16(NFIRST, NDW, data, nadc) # PED_ADC1
                NFIRST = self._skip_block(NFIRST, NDW, data, nadc, 2) # PED_ADC2 unused
                NFIRST = self._skip_block(NFIRST, NDW, data, nsca, 2) # SCALC unused
                NFIRST = self._skip_block(NFIRST, NDW, data, nsca, 2) # SCALS unused
                NFIRST, block_values = self._unpack_block_I16(NFIRST, NDW, data, 4+2+2*nphs)
                gps_data_mid, gps_data_high, _, gps_data_low = block_values[0:4]
            else:
                _ = self._skip_block(NFIRST, NDW, data, 4+16+120*3+128*2, 2) # just verify block size
                NFIRST += 1
                gps_data_mid, gps_data_high, _, gps_data_low = struct.unpack('>4H',data[NFIRST*4:NFIRST*4+8])
                NFIRST += 70
                adc_values = struct.unpack('>120H',data[NFIRST*4:(NFIRST+60)*4])

            gps_truetime_grs = False
            gps_data = ( gps_data_low, gps_data_mid, gps_data_high )
            gps_day_of_year, gps_utc_time_sec, gps_utc_time_str = self._decode_gps(
                gps_data_low, gps_data_mid, gps_data_high)
            
            record.update(dict(
                record_was_decoded  = True,
                run_num             = run_num, 
                frame_num           = frame_num, 
                gps_truetime_grs    = gps_truetime_grs,
                gps_data            = gps_data,
                gps_day_of_year     = gps_day_of_year,
                gps_utc_time_sec    = gps_utc_time_sec,
                gps_utc_time_str    = gps_utc_time_str,
                event_type          = 'pedestal',
                nadc                = nadc,
                adc_values          = adc_values,
            ))

        return record

    def _decode_ruur(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'run')

        NFIRST = self._skip_block(NFIRST, NDW, data, 2, 4) # STATUS

        NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 13)
        run_num = block_values[3]
        sky_quality = block_values[5]
        trig_mode = block_values[6]
        comment_len = block_values[12]

        NFIRST, block_values = self._unpack_block_F32(NFIRST, NDW, data, 7)
        sid_length = block_values[0]

        NFIRST, block_values = self._unpack_block_F64(NFIRST, NDW, data, 2)
        nominal_mjd_start, nominal_mjd_end = block_values

        if(record['gdf_version'] >= 27):
            NFIRST, block_values = self._unpack_block_S(NFIRST, NDW, data, 160)
            observers = self._bytes_to_string(block_values[0][80:])

            NFIRST, block_values = self._unpack_block_S(NFIRST, NDW, data, comment_len)
            comment = self._bytes_to_string(block_values[0])
        else:
            NFIRST += 1
            # Filename would be here but it doesn't seem to be used
            NFIRST += 20
            observers = self._bytes_to_string(data[NFIRST*4:(NFIRST+20)*4])
            NFIRST += 20
            comment = self._bytes_to_string(data[NFIRST*4:(NFIRST*4+comment_len)])

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
           # GDF library ignores HV bank if version < 67
           return record

        NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 4)
        _, mode_code, num_channels, read_cycle = block_values

        status = ()
        v_set = ()
        v_actual = ()
        i_supply = ()
        i_anode = ()
        if(num_channels > 0):
            NFIRST, status = self._unpack_block_I16(NFIRST, NDW, data, num_channels)
            NFIRST, v_set = self._unpack_block_F32(NFIRST, NDW, data, num_channels)
            NFIRST, v_actual = self._unpack_block_F32(NFIRST, NDW, data, num_channels)
            NFIRST, i_supply = self._unpack_block_F32(NFIRST, NDW, data, num_channels)
            NFIRST, i_anode = self._unpack_block_F32(NFIRST, NDW, data, num_channels)

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

        NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 3)
        mode, read_cycle = block_values[1:3]

        NFIRST, block_values = self._unpack_block_I32(NFIRST, NDW, data, 2 if 42<=record['gdf_version']<=64 else 1)
        status = block_values[0]

        NFIRST, block_values = self._unpack_block_F64(NFIRST, NDW, data, 15)
        target_ra, target_dec = block_values[2:4]
        telescope_az, telescope_el, tracking_error = block_values[6:9]
        onoff_offset_ra, onoff_offset_dec, sidereal_time = block_values[9:12]

        NFIRST, block_values = self._unpack_block_S(NFIRST, NDW, data, 80)
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
            target                      = target.strip()    
        ))

        if(record['gdf_version'] > 67):
            # Sidereal time incorrect until after version 67 (OK by v80)
            record.update(dict(
                sidereal_time_hours         = sidereal_time * HRS,
                sidereal_time_hms_str       = self._hms_string(sidereal_time)
            ))

        return record

    def _decode_cccc(self, NDW, data):
        NFIRST, record = self._unpack_gdf_header(data, 'ccd')
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
