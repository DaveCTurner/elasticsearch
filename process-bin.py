import json
import argparse
import re

parser = argparse.ArgumentParser(description='Process binary test output')
parser.add_argument(
  '--input-file',
  dest='input_filename',
  help='bin file to process',
  required=True
  )
parser.add_argument(
  '--output-file',
  dest='output_filename',
  help='results file',
  required=True
  )
args = parser.parse_args()

with open(args.input_filename, 'rb') as fi:
    with open(args.output_filename, 'wb') as fo:
        while True:
            typ = int.from_bytes(fi.read(3))
            if typ == 0:
                break
            siz = 0
            shf = 0
            while True:
                siz0 = int.from_bytes(fi.read(1))
                siz = siz | ((siz0 & 0x7f) << shf)
                shf += 7
                if siz0 & 0x80 == 0:
                    break
            dat = fi.read(siz)
            fo.write(dat)
