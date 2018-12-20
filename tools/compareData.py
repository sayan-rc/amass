#!/usr/bin/env python

import filecmp
import glob
import os
import pickle
import sys

otherdir="/opt/amass/amass"
files = glob.glob(os.path.join("data", '*', '*.pkl'))
sames, diffs = [], []
for file in files:
  otherfile = os.path.join(otherdir, file)
  if filecmp.cmp(file, otherfile):
    sames.append(file)
  else:
    diffs.append(file)

for file in diffs:
  print "Checking file %s" % file
  thisdata = pickle.load(open(file, "rb"))
  otherfile = os.path.join(otherdir, file)
  otherdata = pickle.load(open(otherfile, "rb"))
  for i,row in enumerate(thisdata):
    for j, val in enumerate(row):
      if thisdata[i][j] != otherdata[i][j]:
        print "(%i,%i) this=%s other=%s" % (i,j, str(thisdata[i][j]), str(otherdata[i][j]))
  print len(thisdata)
  print len(otherdata)

if len(diffs) == 0:
  print "No differences found!"
else:
  print "Uh oh, differences found in %i files" % len(diffs)    

