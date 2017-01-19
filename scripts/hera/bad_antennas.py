#! /usr/bin/env python
import sys

arg = sys.argv[1]
f = open(arg+'.bad_ants','w')
f.write(str(81))
f.close()

