# pyfzreader

Python reader of Whipple 10m `GDF/ZEBRA` files, also known as `fz` files. This reader extracts the data written by the Whipple data acquisition system (GRANITE) into Python dictionaries. It has been tested on a small number of files taken between 1998-2000, but should hopefully work for runs before or after this time period. If you find an error while processing a file, please contact me, and I'll try to update the code to resolve the error.

See [Kildea et al., Astroparticle Physics, 28, 2, 182-195, (2007)](https://www.sciencedirect.com/science/article/abs/pii/S0927650507000746) for details of the Whipple 10m system (camera, telescope, DAQ) during the period from 1997-2006, after the so-called GRANITE upgrade program. The article is also [available from its author](https://kildealab.com/publication/elsevier13/elsevier13.pdf) (last downloaded 2024-11-19).

The reader does not depend on any of the CERNLIB system, or on any nonstandard Python packages. The data is extracted by decoding the ZEBRA physical, logical and data-bank structures directly using the Python `struct` package. 

## Usage

There are two ways to use the reader, as a library which allows you to read and process data from `fz` files in your own Python scripts / Jupyter notebooks, or as a script to convert `fz` files into JSON format that can be read by any system that can process JSON.

The library can open `fz` files stored as:

- BZIP2 : with the extension of `.fz.bz2`, using the Python `bz2` package that is part of the Python Standard Library,
- GZIP : with the extension of `.fz.gz`, or `.fzg`, using the Python `gzip` package that is part of the Python Standard Library,
- LZW (UNIX) compress : with the extension of `.fz.Z`, or `.fzz` using the `gunzip` application as a sub-process,
- Uncompressed : any other extension is assumed to be an uncompressed `fz` file which can be read directly by the reader.

### As a standalone JSON converter

To convert a file `gt012345.fz.bz2` into JSON you can use invoke `fzreader.py` directly as:

    python3 fzreader.py -o gt012345.json gt012345.fz.bz2

This file can then be read into Python. For example a crude script to calculate the pedestals and pedestal variances from pedestal events in the run is:

    import json
    import numpy
    nped = 0
    ped_sum = numpy.zeros(492) # Hardcode for 490 pixel camera in this example
    ped_sum_sq = numpy.zeros(492)
    with open('gt012345.json', 'r') as fz:
        for r in json.load(fz):
            if(r['record_type']=='event' and r['event_type']=='pedestal'):
                nped += 1
                ped_sum += numpy.asarray(r['adc_values'])
                ped_sum_sq += numpy.asarray(r['adc_values'])**2
    ped_val = ped_sum/nped
    ped_rms = numpy.sqrt(ped_sum_sq/nped - ped_val**2)

### Integrated directly into your analysis scripts

The library can be used to read `fz` files directly, skipping the conversion to JSON. For example the script above can be rewritten as:

    import fzreader
    import numpy
    nped = 0
    ped_sum = numpy.zeros(492) # Hardcode for 490 pixel camera in this example
    ped_sum_sq = numpy.zeros(492)
    with fzreader.FZReader('gt012345.fz.bz2') as fz:
        for r in fz:
            if(r['record_type']=='event' and r['event_type']=='pedestal'):
                nped += 1
                ped_sum += numpy.asarray(r['adc_values'])
                ped_sum_sq += numpy.asarray(r['adc_values'])**2
    ped_val = ped_sum/nped
    ped_rms = numpy.sqrt(ped_sum_sq/nped - ped_val**2)

## Generating images of events

The Python notebook `Draw events.pynb` gives an example of how to use the reader to render events in a camera view. The notebook first loops through all events in a given `fz` file to calculate the mean pedestal in each channel, then reopens the file drawing the signal in each channel as a camera view. For simplicity, it only supports the 490 pixel camera.

![Image of an event in the Whipple 490 pixel camera, possibly a muon.](https://github.com/Whipple10m/pyfzreader/blob/main/Assets/gt014862_ev00211.png)

## Format of data records

The following types of data record are supported by the reader and decoded into a Python dictionary: `Run Header`, `10m event`, `HV measurement`, `Tracking status`. In addition, the `10m frame` is recognized by the reader but no data is decoded, and a minimal dictionary is returned as described below. All other records result in the return a dictionary with the following elements:

- `'record_type'` : `'unknown'`
- `'bank_id'` : Four character string describing data bank from the ZEBRA file. This corresponds to the bank name given in [GDF FORTRAN code](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L1048), for example `'CCCC'` would correspond to the CCD bank.

For each of the five supported record types the reader will result in a dictionary containing at least the following items:

- `'record_type'`: `'run'`, `'event`', `'tracking'`, `'hv'`, or `'frame'`.
- `'record_time_mjd'`: the MJD associated with the GDF record. This is seemingly generated by the DAQ system `GRANITE`, possibly from the system time on the computer. I do not know how accurate it is. Zero is returned if the MJD is out of range.
- `'record_time_str'`: the MJD from the record translated into a UTC string in the form `'YYYY-MM-DD hh:mm:ss.sss'`. The string `'unknown'` is returned if the MJD is out of range.
- `'gdf_version'`: the version of the GDF library used to write the `fz` file. For example, Whipple data written in 2000 has version 83. See the header for [gdf.for](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L93) for details.
- `'record_was_decoded'`: `True` if the reader was able to fully decode the GDF record. If this is the case further elements will be returned in the dictionary as discussed below. `False` otherwise, in which case no further elements will be returned.

### Run header

The [GDF run header structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L263) is decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'run'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above.
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `True`, the reader can decode all known versions of the GDF run header.
- `'run_num'`: the run number
- `'sky_quality'`: the sky quality noted by the observers, if they remembered to update it. Should be `A`, `B` or `C`, but can also be `?` if the value is invalid.
- `'sid_length`': the nominal length of the run in sidereal minutes.
- `'trigger_mode'`: value that describes the trigger mode (I don't recall what it corresponds to).
- `'nominal_mjd_start'`: nominal start time of the run in MJD
- `'nominal_mjd_end'`: nominal end time of the run in MJD
- `'observers'`: string listing observers on shift that night. Not necessarily updated every night in the DAQ system, the logsheet is a better reference to who was observing.
- `'comment'`: comment entered into the DAQ system by observers.

Other data items present in the FORTRAN structure are not decoded by the reader as they don't seem to be relevant for the data files that I have. Please contact me if you need any of them to be extracted.

### 10m event

The [GDF 10m event structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L458) is decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'event'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above. 
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `True`, the reader can decode all known versions of the GDF 10m event.
- `'run_num'`: see above.
- `'event_num'`: event number, starting at zero.
- `'livetime_sec'`: number of seconds counted by 10MHz livetime scaler since start of run. The livetime scaler is gated by the system veto, so it only counts time when the trigger is ready to receive an event.
- `'livetime_ns'`: number of nanoseconds counted by 10MHz livetime scaler since last 1-second marker.
- `'elaptime_sec'`: number of seconds counted by 10MHz elapsed time scaler since start of run. Only present if `gdf_version>=74`.
- `'elaptime_ns'`: number of nanoseconds counted by 10MHz elapsed time scaler since last 1-second marker. Only present if `gdf_version>=74`.
- `'grs_data'`: raw data provided by GRS clock decoder. Only present if `gdf_version>=74`.
- `'grs_doy'`: the day of year decoded from the GRS system. Only present if `gdf_version>=74`.
- `'grs_utc_time_sec'`: the number of seconds and fraction of seconds decoded from the GRS system. Only present if `gdf_version>=74`.
- `'grs_utc_time_str'`: the time from the GRS system in string format `'hh:mm:ss.sssssss'`. Only present if `gdf_version>=74`.
- `'gps_clock'`: raw data provided by old (pre-GRS) clock. Meaning unknown. Only present if `gdf_version<74`.
- `'event_type`': either `'pedestal'` or `'sky'`
- `'nadc'`: the number of ADC channels in the event. The ADCs each have 12 channels, so this will usually be larger than the number of pixels in the camera. For the 331 pixel camera `nadc=336`, for the 490 pixel camera `nadc=492`.
- `'ntrigger'`: number of trigger words in the event. This depends on what epoch the data comes from, before the Leeds PST or not, and whether trigger readout is enabled. Only present if `gdf_version>=74`.
- `'trigger_data`': array of size `'ntrigger'` giving the trigger data words (I32). Only present if `gdf_version>=74`.
- `'adc_values'`: array of size `'nadc'` giving the ADC values (I16)

Other data items present in the FORTRAN structure are not decoded by the reader as they don't seem to be relevant for the data files that I have. Please contact me if you need any of them to be extracted.

### Tracking status

The [GDF tracking-record structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L375) is decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'tracking'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above. 
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `True` if `gdf_version>=80`. Otherwise `False`, in which case the following elements will not be in the dictionary.
- `'mode`': tracking mode, one of `'on'`, `'off'`, `'slewing'`, `'standby`, `'zenith'`, `'check'`, `'stowing'`, `'drift'`, or `'unknown'`.
- `'mode_code'`: integer code corresponding to mode
- `'read_cycle'`: integer giving cycle number of information transferred by tracking system.
- `'status'`: bit pattern giving tracking status (values unknown)
- `'target_ra_hours'`: right-ascension of target in hours from 0.0 to 24.0.
- `'target_ra_hms_str`': right-ascension of target as printable string in format `'HHhMMmSS.Ss'`, e.g. `'12h34m56.7s'`.
- `'target_dec_deg`': declination of target in degrees from -90.0 to +90.0.
- `'target_dec_dms_str`': declination of target as printable string in format `'+/-DDDdMMmSS.Ss'`, e.g. `'+12h34m56.7s'`
- `'telescope_az_deg'`: telescope azimuth angle in degrees.
- `'telescope_el_deg'`: telescope elevation angle in degrees.
- `'tracking_error_deg'`: estimated error between target and telescope in degrees.
- `'onoff_offset_ra_hours'`: right-ascension offset between ON and OFF runs in hours.
- `'onoff_offset_ra_hms_str'`: right-ascension offset between ON and OFF runs as printable string in format `'HHhMMmSS.Ss'`, e.g. `'00h30m00.0s'`
- '`onoff_offset_dec_deg`': declination offset between ON and OFF runs in degrees.
- `'onoff_offset_dec_dms_str'`: declination offset between ON and OFF runs as printable string in format `'+/-DDDdMMmSS.Ss'`, e.g. `'+00h00m00.0s'`
- `'sidereal_time_hours'`: local sidereal time in hours.
- `'sidereal_time_hms_str'`: local sidereal time as printable string in format `'HHhMMmSS.Ss'`, e.g. `'12h34m56.7s'`.
- `'target'`: string giving name of target

Other data items present in the FORTRAN structure are not decoded by the reader as they don't seem to be relevant for the data files that I have. Please contact me if you need any of them to be extracted.

### High-voltage status

The [GDF HV-record structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L303) is decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'hv'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above. 
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `True` if `gdf_version>=67`. Otherwise `False`, in which case the following elements will not be in the dictionary.
- `'mode_code'`: integer value corresponding to operating mode (values unknown).
- `'num_channels'`: number of HV channel values stored in record.
- `'read_cycle'`: integer giving cycle number of information transferred by HV system.
- `'status'`: array of size `'num_channels'` giving status of HV system for this channel. This is a bit-field described in the [GDF FORTRAN code](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L205).
- `'v_set'`: array of size `'num_channels'` giving voltage set in each channel (negative value given).
- `'v_actual'`: array of size `'num_channels'` giving voltage measured in each channel (negative value given).
- `'i_supply'`: array of size `'num_channels'` giving measured power-supply current in each channel.
- `'i_anode'`: array of size `'num_channels'` giving measured anode current in each channel.

Other data items present in the FORTRAN structure are not decoded by the reader as they don't seem to be relevant for the data files that I have. Please contact me if you need any of them to be extracted.

### 10m frame

The [GDF 10m frame structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L413) is decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'frame'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above. 
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `False`, the reader does not decode the 10m frame records.

Other data items present in the FORTRAN structure are not decoded by the reader as they don't seem to be relevant for the data files that I have. Please contact me if you need any of them to be extracted.

## Understanding the reader

To understand how the reader functions under the hood it may be useful to refer to the "Overview of the ZEBRA System" (CERN Program Library Long Writeups Q100/Q101), and in particular Chapter 10, which describes the layout of the physical, logical and data headers in "exchange mode".
 
https://cds.cern.ch/record/2296399/files/zebra.pdf

The format Whipple specific data structures, written to the ZEBRA data banks, can be extracted from the GDF code, written by Joachim Rose at Leeds, which directs the writing of the individual data elements in blocks of data all of whom have the same data type (blocks of I32, blocks of I16 etc.). See for example the function GDF$EVENT10 and observe the calls to GDF$MOVE.

https://github.com/Whipple10m/GDF/blob/main/gdf.for
