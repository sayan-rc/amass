#!/usr/bin/python 

import os
import pickle
import sys

if len(sys.argv) < 2:
    sys.stderr.write("Must specify input file")
    sys.exit(1)
pkl_file = sys.argv[1]
if not os.path.isfile(pkl_file):
    sys.stderr.write("%s is not a file" % pkl_file)
    sys.exit(1)
data = pickle.load( open( pkl_file, "rb" ) )
print data
