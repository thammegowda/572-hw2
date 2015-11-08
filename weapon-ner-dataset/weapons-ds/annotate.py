#!/usr/bin/env python2.7
import sys

inp_f = "weapon-types.tok"
out_f = "weapon-types-tagged.tok"

tag = "WPNTP"
otag = "O"
otokens = set(["start", "end"])

with open(inp_f) as rd:
    with open(out_f, 'w') as wt:
        for t in rd:
            t = t.strip()
            if t:
                wt.write("%s\t%s\n" % (t, tag if t not in otokens else otag))
print "Done"
            
