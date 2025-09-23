# pyfzreader

**New in 2025:** The Whipple data from 1995 to 2011 is available publicly from a repository hosted both on the [Harvard dataverse (DOI:10.7910/DVN/VSXO03)](https://doi.org/10.7910/DVN/VSXO03) and on [Zenodo (DOI:10.5281/zenodo.16890875)](https://doi.org/10.5281/zenodo.16890875).

Python reader of Whipple 10m `GDF/ZEBRA` files, also known as `fz` files. This reader extracts the data written by the Whipple data acquisition system (`Granite`) into Python dictionaries. It has been tested on more than 20,000 files taken between 1995 and 2011, and works for the vast majority of them. In certain cases (less than 3% of runs) the data file is truncated or malformed (e.g. it may originally have been transferred from the data acquisition system in ASCII mode) and the reader may raise `EOFError` or `FZDecodeError`. If you find an error while processing a file, please contact me, and I can check if there is a problem with the data file and/or whether it is possible to update the code to resolve the error.

See [Kildea et al., Astroparticle Physics, 28, 2, 182-195, (2007)](https://www.sciencedirect.com/science/article/abs/pii/S0927650507000746) for details of the Whipple 10m system (camera, telescope, DAQ) during the period from 1997-2006, after the so-called GRANITE upgrade program. The article is also [available from its author](https://kildealab.com/publication/elsevier13/elsevier13.pdf) (last downloaded 2024-11-19).

The reader does not depend on any of the CERNLIB system, or on any nonstandard Python packages. The data is extracted by decoding the ZEBRA physical, logical, and data-bank structures directly using the Python `struct` package. 

## Usage

There are two ways to use the reader: as a library which allows you to read and process data from `fz` files in your own Python scripts / Jupyter notebooks, or as a script to convert `fz` files into JSON format that can be read by any system that can process JSON.

The library can open `fz` files stored as:

- BZIP2: with the extension of `.fz.bz2`, using the Python `bz2` package that is part of the Python Standard Library,
- GZIP: with the extension of `.fz.gz`, or `.fzg`, using the Python `gzip` package that is part of the Python Standard Library,
- LZW (UNIX) compress: with the extension of `.fz.Z`, or `.fzz` using the `gunzip` application as a sub-process,
- Uncompressed: any other extension is assumed to be an uncompressed `fz` file which can be read directly by the reader.

### As a standalone JSON converter

To convert a file `gt012345.fz.bz2` into JSON you can invoke `fzreader.py` directly as:

    python3 fzreader.py -o gt012345.json gt012345.fz.bz2

This file can then be read into Python. For example, a crude script to calculate the pedestals and pedestal variances from pedestal events in the run is:

    import json
    import numpy
    nped = 0
    ped_sum = 0
    ped_sum_sq = 0
    with open('gt012345.json', 'r') as fz:
        for r in json.load(fz):
            if r['record_type'] in ('event', 'frame') and r['record_was_decoded']==True and r['event_type']=='pedestal':
                adc = numpy.asarray(r['adc_values'])
                nped += 1
                ped_sum = numpy.add(adc, ped_sum)
                ped_sum_sq = numpy.add(adc**2, ped_sum_sq)
    ped_val = ped_sum/nped
    ped_rms = numpy.sqrt(ped_sum_sq/nped - ped_val**2)

### Integrated directly into your analysis scripts

The library can be used to read locally hosted `fz` files directly, skipping the conversion to JSON. For example, the script above can be rewritten as:

    import fzreader
    import numpy
    nped = 0
    ped_sum = 0
    ped_sum_sq = 0
    with fzreader.FZReader('gt012345.fz.bz2') as fz:
        for r in fz:
            if fzreader.is_pedestal_event(r):
                adc = numpy.asarray(r['adc_values'])
                nped += 1
                ped_sum = numpy.add(adc, ped_sum)
                ped_sum_sq = numpy.add(adc**2, ped_sum_sq)
    ped_val = ped_sum/nped
    ped_rms = numpy.sqrt(ped_sum_sq/nped - ped_val**2)

where we have also used the `fzreader.is_pedestal_event(record)` function to replace the three-part test in the previous version.

### Using the public data archive  hosted on the Harvard dataverse or Zenodo

The library can read raw data files and logsheets directly from either of the public Whipple repositories. For example, the script above can be adapted to use the Zenodo archive:

    import fzreader
    import numpy
    nped = 0
    ped_sum = 0
    ped_sum_sq = 0
    archive = fzreader.FZDataArchive('zenodo', verbose=True) # can also choose 'harvard'
    fzdata = archive.get_run_by_number(12345)
    with fzreader.FZReader(fzdata) as fz:
        for r in fz:
            if fzreader.is_pedestal_event(r):
                adc = numpy.asarray(r['adc_values'])
                nped += 1
                ped_sum = numpy.add(adc, ped_sum)
                ped_sum_sq = numpy.add(adc**2, ped_sum_sq)
    ped_val = ped_sum/nped
    ped_rms = numpy.sqrt(ped_sum_sq/nped - ped_val**2)

## Generating images of events

The Python notebook `Draw events.pynb` gives an example of how to use the reader to render events in a camera view. The notebook first loops through all events in a given `fz` file to calculate the mean pedestal in each channel, then reopens the file drawing the signal in each channel as a camera view. The notebook supports all Whipple cameras from the GRANITE epoch.

![Images of muon events in various Whipple camera.](https://github.com/Whipple10m/pyfzreader/blob/main/Assets/muon_images.png)

## Analysis of Markarian flare from 1996-05-07

The Python notebook `Analyze Mrk 421 flare from 07 May 1996.ipynb` provides an example of using the reader to analyze five observation runs taken on the [AGN](https://en.wikipedia.org/wiki/Active_galactic_nucleus) Markarian 421 on the night of 07 May 1996, when a large flare of VHE gamma rays was detected by the Whipple 10m. The notebook starts by downloading the losgheet for the night, the data files corresponding to the five observations, and a flat-fielding calibration run from the night from the Zenodo archive. It then applies a crude calibration and parameterization of the images and applies a set of selection criteria (Supercuts 1995) to reject the background of cosmic rays and illustrates the gamma-ray signal as an *alpha plot*, shown below. The peak at low values of *alpha* shows an excess of events that are consistent with the source over the level of the background at *alpha* values between 20 and 90 degrees.

![Alpha plot showing gamma-ray signal from Mrk 421.](https://github.com/Whipple10m/pyfzreader/blob/main/Assets/mrk421_alpha.png)

## Format of data records

The following types of data records are supported by the reader and decoded into a Python dictionary: `Run Header`, `10m event`, `HV measurement`, `Tracking status`, and `10m frame`. In addition, `CCD data` records are recognized by the reader but no data is decoded, and a minimal dictionary is returned as described below. All other records result in the return of a dictionary with the following elements:

- `'record_type'`: `'unknown'`
- `'bank_id'`: Four character string describing the data bank from the ZEBRA file. This corresponds to the bank name given in [GDF FORTRAN code](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L1048), for example `'MEEM'` would correspond to the Monte-Carlo event bank.

For each of the six recognized record types, the reader will return a dictionary containing at least the following items:

- `'record_type'`: `'run'`, `'event'`, `'tracking'`, `'hv'`, `'frame'`, or `'ccd'`.
- `'record_time_mjd'`: the MJD associated with the GDF record. This is seemingly generated by the DAQ system `Granite`, possibly from the system time on the computer. I do not know how accurate it is. Zero is returned if the MJD is out of range.
- `'record_time_str'`: the MJD from the record translated into a UTC string in the form `'YYYY-MM-DD hh:mm:ss.sss'`. The string `'unknown'` is returned if the MJD is out of range.
- `'gdf_version'`: the version of the GDF library used to write the `fz` file. For example, Whipple data written in 2000 has version 83. See the header for [gdf.for](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L93) for details.
- `'record_was_decoded'`: `True` if the reader was able to fully decode the GDF record. If this is the case, further elements will be returned in the dictionary as discussed below. `False` otherwise, in which case no further elements will be returned.

### Run header

The [GDF run header structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L263) is decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'run'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above.
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `True`, the reader can decode all known versions of the GDF run header.
- `'run_num'`: the run number.
- `'sky_quality'`: the sky quality noted by the observers, if they remembered to update it. Should be `'A'`, `'B'` or `'C'`, but can also be `'?'` if the value is invalid.
- `'sid_length'`: the nominal length of the run in sidereal minutes.
- `'trigger_mode'`: value that describes the trigger mode (I don't recall what it corresponds to).
- `'nominal_mjd_start'`: nominal start time of the run in MJD.
- `'nominal_mjd_end'`: nominal end time of the run in MJD.
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
- `'gps_system'`: the GPS system from which the timestamp is derived, one of `'michigan'`, `'grs'`, or '`hytec'`, as discussed in the section on timestamps, below.
- `'gps_data'`: raw data provided by GRS clock. Format depends on clock system used (see `'gps_system'` element, above).
- `'gps_mjd'`: the integer MJD fully or partially decoded by the GPS system, as discussed in the section on clocks, below. 
- `'gps_utc_sec'`: the integer number of seconds since UTC midnight, as decoded by the GPS system.
- `'gps_ns'`: fraction of the second, whose precision depends on the GPS system used, expressed as nanoseconds.
- `'gps_utc_time_str'`: the time from the GPS system in string format `'hh:mm:ss.sssssss'`.
- `'gps_is_good'`: flag indicating that the GPS timestamp *might* be good.
- `'event_type'`: either `'pedestal'` or `'sky'`.
- `'nadc'`: the number of ADC channels in the event. The ADCs each have 12 channels, so this will usually be larger than the number of pixels in the camera. For the 331 pixel camera `nadc=336`, for the 490 pixel camera `nadc=492`.
- `'adc_values'`: array of size `'nadc'` giving the ADC values (I16).
- `'ntrigger'`: number of trigger words in the event. This depends on what epoch the data comes from, before the Leeds PST or not, and whether trigger readout is enabled. Only present if `gdf_version>=74`.
- `'trigger_data'`: array of size `'ntrigger'` giving the trigger data words (I32). Only present if `gdf_version>=74`.
- `'all_values'`: dictionary of all values found in the GDF event structure, if the `'return_all_values'` option has been specified in the class.

By default, the reader only extracts the elements of the record that seemed relevant to me, given the epoch of the data. The `'return_all_values'` can be used to force the reader to extract all data in the GDF record, irrespective of whether they are being written with valid data. Please contact me if you would like any of these values returned by default.

### 10m frame

Before GDF version 80, the on-the-fly calibration data were recorded in the [GDF 10m frame structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L413), with the pedestal event ADC values being the most important component. The frame records are partially decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'frame'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above. 
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `True` if `gdf_version<80`. Otherwise, `False`, in which case none of the following elements will be present, except `'all_values'` if the `'return_all_values'` has been selected.
- `'run_num'`: see above.
- `'frame_num'`: event number, starting at one.
- `'gps_system'`: `'michigan'`, see above.
- `'gps_data'`: see above.
- `'gps_mjd'`: see above.
- `'gps_utc_sec'`: see above.
- `'gps_utc_time_str'`: see above.
- `'gps_is_good'`: see above.
- `'event_type'`: `'pedestal'`.
- `'nadc'`: see above.
- `'adc_values'`: the ADC values in the `'ped_adc1'` sector of the GDF record.
- `'all_values'`: dictionary of all values found in the GDF frame structure, if the `'return_all_values'` option has been specified in the class.

By default, the reader only extracts the elements of the record that seemed relevant to me, given the epoch of the data. The `'return_all_values'` can be used to force the reader to extract all data in the GDF record, irrespective of whether they are being written with valid data. Please contact me if you would like any of these values returned by default.

### Tracking status

The [GDF tracking-record structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L375) is decoded into a Python dictionary that contains the following items:

- `'record_type'`: `'tracking'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above. 
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `True`, the reader can decode all known versions of the GDF tracking data.
- `'mode'`: tracking mode, one of `'on'`, `'off'`, `'slewing'`, `'standby'`, `'zenith'`, `'check'`, `'stowing'`, `'drift'`, or `'unknown'`.
- `'mode_code'`: integer code corresponding to mode.
- `'read_cycle'`: integer giving cycle number of information transferred by tracking system.
- `'status'`: bit pattern giving tracking status (values unknown).
- `'target_ra_hours'`: right-ascension of target in hours from 0.0 to 24.0.
- `'target_ra_hms_str'`: right-ascension of target as printable string in format `'HHhMMmSS.Ss'`, e.g. `'12h34m56.7s'`.
- `'target_dec_deg'`: declination of target in degrees from -90.0 to +90.0.
- `'target_dec_dms_str'`: declination of target as printable string in format `'+/-DDdMMmSSs'`, e.g. `'+12h34m56.7s'`.
- `'telescope_az_deg'`: telescope azimuth angle in degrees.
- `'telescope_el_deg'`: telescope elevation angle in degrees.
- `'tracking_error_deg'`: estimated error between target and telescope in degrees.
- `'onoff_offset_ra_hours'`: right-ascension offset between ON and OFF runs in hours.
- `'onoff_offset_ra_hms_str'`: right-ascension offset between ON and OFF runs as printable string in format `'HHhMMmSS.Ss'`, e.g. `'00h30m00.0s'`.
- `'onoff_offset_dec_deg'`: declination offset between ON and OFF runs in degrees.
- `'onoff_offset_dec_dms_str'`: declination offset between ON and OFF runs as printable string in format `'+/-DDDdMMmSS.Ss'`, e.g. `'+00h00m00.0s'`.
- `'sidereal_time_hours'`: local sidereal time in hours. Only present if `gdf_version>67`.
- `'sidereal_time_hms_str'`: local sidereal time as printable string in format `'HHhMMmSS.Ss'`, e.g. `'12h34m56.7s'`. Only present if `gdf_version>67`.
- `'target'`: string giving name of target.

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

### CCD data

The [GDF CCD structure](https://github.com/Whipple10m/GDF/blob/24572fc741a8f360979dd816c0fdd3b668558353/gdf.for#L337) is recognized by the reader but not decoded. The following Python dictionary is returned:

- `'record_type'`: `'ccd'`.
- `'record_time_mjd'`: see above.
- `'record_time_str'`: see above. 
- `'gdf_version'`: see above.
- `'record_was_decoded'`: `False`, the reader does not decode the CCD records.

Please contact me if you need the CCD data to be extracted.

## Timestamping

A set of scalers and GPS timestamping systems were used to measure the live time, elapsed time, and absolute time of the events recorded at Whipple. This section is intended only as a brief outline of these, sufficient to describe the timing values that the GDF reader provides.

A 10MHz clock, derived locally from a frequency generator, was counted by two 48-bit scalers. The first was gated by the global veto, providing a running estimate of the total livetime in the run, available as `'livetime_sec'` and `'livetime_ns'` in the dictionary for the `event` and `frame` records. The second was an ungated count of the oscillator, providing the total elapsed time since the start of the run, only available for starting from GDF version 74, corresponding to run number 9042, and available as `'elaptime_sec'` and `'elaptime_sec'` in the record dictionary. The first step of any analysis was usually to calibrate the 10MHz oscillator frequency against the GPS system using the one-pulse per second (1PPS) markers from the GPS.

Three different absolute GPS timestamping systems were used during the GRANITE epoch, two based on a Truetime GPS clock (initially XL-DC then XL-AK), and the third a dedicated Hytec timestamp module.

1. Before the 1997 observing season, i.e. up until approximately September 1997, and corresponding to run numbers less than approximately 9000 or GDF versions before 80, the Truetime XL-DC was read out by a set of CAMAC modules known as the `Michigan` clock interface, and described by [Freeman and Akerlof, NIM A320, 305-309, 1992](https://deepblue.lib.umich.edu/bitstream/handle/2027.42/29901/0000258.pdf). The GPS time was provided by the XL-DC over a 48-bit wide parallel port, with quarter millisecond resolution, and digitized by a CAMAC module, from which three 16-bit values are stored in the GDF record. The system provided a BCD coded UTC time in hours, minutes, seconds and milliseconds, a quarter millisecond count, the day of the year (DOY), and some status bits.

2. Between 1997-10-29 (run number 9042) and 2006-06-19 (run number 31781) the XL-DC (initially), and a Truetime XL-AK clock (subsequently) were digitized by a [Wisconsin GRS2 module](https://github.com/Whipple10m/Documentation/blob/main/components/gpsmanual2a.pdf) which counted the local 10MHz frequency described above, and combined that with the GPS time read out from the Truetime serial port, as described in the GRS2 manual. These values were stored as three 32-bit values in the GDF record, providing the BCD coded UTC time in hours, minutes and seconds, the number of 10MHz ticks since the last second marker, the DOY, and some status bits. The serial connection between the GRS2 and the Truetime ceased functioning after 2006-06-19 (run 31781) but the module remained in the system until 2008-01-15 (being removed after run 34171). No absolute time is available during this period.

3. From 2008-01-15 (run 34172) a Hytec timestamping module (GPS92) was used to provide absolute timestamps. This system was interpreted by the data acquisition system which wrote the integer MJD, number of seconds since GPS (or UTC?) midnight, and the number of nanoseconds since the 1PPS into the GDF record. The Hytec GPS was non-functional between 2008-04-25 (run 34973) and 2009-06-04 (run 36727) inclusive due to antenna damage. After repair, it functioned until the end of data taking with the 10m on 2011-05-31.

To insulate users of the `fzreader` from these details, the GPS data relevant to each epoch is used to provide the timestamp in a consistent format. For the epoch of the Truetime GPS (`Michigan` and `GRS2`) the GPS DOY is combined with an estimate of the year generated from the run number, using a hardcoded lookup table, to calculate the MJD, which is combined with the UTC second number and fraction of the second. For the `Hytec` epoch, the values provided in GDF are used directly, corrected for the difference between GPS and UTC times (number of leap seconds).

## Understanding the reader

To understand how the reader functions under the hood it may be useful to refer to the "Overview of the ZEBRA System" (CERN Program Library Long Writeups Q100/Q101), and in particular Chapter 10, which describes the layout of the physical, logical and data headers in "exchange mode".
 
https://cds.cern.ch/record/2296399/files/zebra.pdf

The format of Whipple specific data structures, written to the ZEBRA data banks, can be extracted from the GDF code, written by Joachim Rose at Leeds, which directs the writing of the individual data elements in blocks of data all of whom have the same data type (blocks of I32, blocks of I16 etc.). See for example the function GDF$EVENT10 and observe the calls to GDF$MOVE.

https://github.com/Whipple10m/GDF/blob/main/gdf.for
