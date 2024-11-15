# pyfzreader

Python reader of Whipple `GDF/ZEBRA` files, also known as `fz` files. This reader extracts the data written by the Whipple data acquisition system (GRANITE) into Python dictionaries. It has been tested on a small number of files taken between 1998-2000, but should hopefully work for runs before or after this time period. If you find an error while processing a file, please contact me and I'll try to update resolve the error.

The reader does not depend on any of the CERNLIB system, or on any nonstandard Python packages. The data is extracted by decoding the ZEBRA physical, logical and data-bank structures directly using the Python `struct` package. 

## Usage ##

There are two ways to use the reader, as a library which allows you to read and process data from `fz` files in your own Python scripts / Jupyter notebooks, or as a script to convert `fz` files into JSON format that can be read by any system that can process JSON.

### As a standalone JSON converter ###

To convert a file `gt012345.fz` into JSON you can use invoke `fzreader.py` directly as:

    python3 fzreader.py -o gt012345.json gt012345.fz

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

### Integrated directly into your analysis scripts ###

The library can be used to read `fz` files directly, skipping the conversion to JSON. For example the script above can be rewritten as:

    import fzreader
    import numpy
    nped = 0
    ped_sum = numpy.zeros(492) # Hardcode for 490 pixel camera in this example
    ped_sum_sq = numpy.zeros(492)
    with fzreader.FZReader('gt012345.fz') as fz:
        for r in fz:
            if(r['record_type']=='event' and r['event_type']=='pedestal'):
                nped += 1
                ped_sum += numpy.asarray(r['adc_values'])
                ped_sum_sq += numpy.asarray(r['adc_values'])**2
    ped_val = ped_sum/nped
    ped_rms = numpy.sqrt(ped_sum_sq/nped - ped_val**2)

## Understanding the reader ##

To understand how the reader functions it may be useful to refer to the "Overview of the ZEBRA System" (CERN Program Library Long Writeups Q100/Q101), and in particular Chapter 10, which describes the layout of the physical, logical and data headers in "exchange mode".
 
https://cds.cern.ch/record/2296399/files/zebra.pdf

The format Whipple specific data structures, written to the ZEBRA data banks, can be extracted from the GDF code, written by Joachim Rose at Leeds, which directs the writing of the individual data elements in blocks of data all of whom have the same data type (blocks of I32, blocks of I16 etc.). See for example the function GDF$EVENT10 and observe the calls to GDF$MOVE.

https://github.com/Whipple10m/GDF/blob/main/gdf.for

