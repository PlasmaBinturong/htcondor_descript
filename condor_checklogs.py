#!/usr/bin/env python2.7

"""Output logfiles indicating an error in the last run.

USAGE: ./condor_logs_check.py *condor.log
"""


import sys
import re


RE_RETURN = re.compile(r'\(return value (\d+)\)')
#RE_SUBMITTED = re.compile(r'000 ')
#RE_STARTED = re.compile(r'001 ')
#RE_TERM = re.compile(r'005 ')

states = {'000 ': 'submitted',
          '001 ': 'started',
          '005 ': 'terminated'}

def termination_code(logfile):
    """get return value of last run and check whether it was non-zero
    (return False if non-zero)"""
    state = None

    termination_match = None
    with open(logfile) as log:
        for line in log:
            state_code = line[:4]
            state = states.get(state_code) or state

            m = RE_RETURN.search(line)
            if m:
                termination_match = m
    try:
        return_value =  int(termination_match.group(1))
    except AttributeError:
        return_value = None

    return state, return_value
        


if __name__=='__main__':
    logfiles = sys.argv[1:]
    if not logfiles:
        print >>sys.stderr, __doc__
        sys.exit(1)

    count_failed = 0
    count_noterm = 0
    for logfile in logfiles:
        state, return_value = termination_code(logfile)
        if state:
            if state == 'terminated':
                if return_value != 0:
                    print "Termination with error: %2d : %s" % (return_value,
                                                                logfile)
                    count_failed += 1
            else:
                print "Not terminated (%s): %s" % (state, logfile)
                count_noterm += 1
        else:
            print >>sys.stderr, "WARNING: Invalid log file: %s" % logfile

    print "%d failed, %d not terminated (total: %d) " % (count_failed,
                                                         count_noterm,
                                                         len(logfiles))




