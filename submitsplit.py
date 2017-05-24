#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Split a condor description file with many blocks. 
Useful when there are too many blocks to be submitted at once.
Although the submission might work up to 18000 blocks, it can be a good idea to
take 5000 as an upper limit"""

import argparse
import os.path

MAX_NBLOCKS = 5000


def read_blocks(descfile):
    blocks = []
    nextblock = ''
    with open(descfile) as desc:
        for line in desc:
            if not line.rstrip():
                if nextblock:
                    blocks.append(nextblock)
                    nextblock = ''
            else:
                nextblock += line
    return blocks


def submitsplit(descfile, nparts=None, nblocks=MAX_NBLOCKS):
    outbase, outext = os.path.splitext(descfile)
    out_template = outbase + '-part%d' + outext
    mainblock, *blocks = read_blocks(descfile)
    N = len(blocks)
    nparts = nparts or (N // nblocks + 1)
    n = N // nparts
    i = 1
    while i <= nparts:
        with open(out_template % i, 'w') as out:
            out.write(mainblock + '\n')
            for block in blocks[(i-1)*n:i*n]:
                out.write(block + '\n')
        i += 1



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('descfile')
    parser.add_argument('-p', '--nparts', type=int,
                        help='how many output files [%(default)s]')
    parser.add_argument('-b', '--nblocks', default=MAX_NBLOCKS, type=int,
                        help='Maximum number of blocks per file [%(default)s]')
    
    args = parser.parse_args()
    submitsplit(**vars(args))
