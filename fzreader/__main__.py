from . import FZReader

import json
import getopt
import sys

def main():
    output_file = None  # Default to stdout
    verbose = False
    verbose_file = None
    args = sys.argv[1:]
    try:
        opts, args = getopt.getopt(args, "vo:d:", ["output=", "debug="])
    except getopt.GetoptError as e:
        print(str(e), file=sys.stderr)
        print("Usage: python3 -m fzreader [-o <output_file> | --output=<output_file>] [-d <debug_file> | --debug=<debug_file>] <input_file.fz>", file=sys.stderr)
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
        print("Usage: python3 -m fzreader [-o <output_file> | --output=<output_file>] [-d <debug_file> | --debug=<debug_file>] <input_file.fz>", file=sys.stderr)
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


if __name__ == "__main__":
    main()
