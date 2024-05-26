import json
import argparse
import re

parser = argparse.ArgumentParser(description='Process binary test output')
parser.add_argument(
  '--file',
  dest='filename',
  help='file to parse',
  required=True
  )
args = parser.parse_args()

with open(args.filename, 'rb') as f:
    pos = 0
    while True:
        startPos = pos
        typ = int.from_bytes(f.read(3))
        pos += 3
        if typ == 0x000100:
            siz = int.from_bytes(f.read(1))
            pos += 1
        elif typ == 0x010100:
            siz = int.from_bytes(f.read(1))
            skp = int.from_bytes(f.read(1))
            pos += 2
            print("skp={:02X}".format(skp))
        elif typ == 0x010102 or typ == 0x000102 or typ == 0x010103:
            siz = 0
            shf = 0
            while True:
                siz0 = int.from_bytes(f.read(1))
                pos += 1
                siz = siz | ((siz0 & 0x7f) << shf)
                shf += 7
                if siz0 & 0x80 == 0:
                    break
        else:
            print("pos={:016X} typ={:06X} unknown".format(startPos, typ))
            raise
        print("pos={:016X} typ={:06X} siz={:04X}".format(startPos, typ, siz))
        dat = f.read(siz)
        print(dat)
        pos += siz

