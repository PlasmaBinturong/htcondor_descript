#!/usr/bin/env python2.7


"""Output logfiles indicating an error in the last run."""

# USAGE: ./condor_logs_check.py *condor.log

from __future__ import print_function


import re
import argparse
import logging
logger = logging.getLogger(__name__)


RE_RETURN = re.compile(r'\(([^()]+) (\d+)\)')
RE_DATE = re.compile(r'0\d\d \([0-9.]+\) (\d+/\d+ \d+:\d+:\d+) ')
#RE_SUBMITTED = re.compile(r'000 ')
#RE_STARTED = re.compile(r'001 ')
#RE_TERM = re.compile(r'005 ')

STATES = {'000 ': 'submitted',
          '001 ': 'started',
          '004 ': 'evicted',
          '005 ': 'terminated',
          #'006 ': 'Image size updated',
          '009 ': 'aborted',
          '022 ': 'disconnected',  # attempting to reconnect
          '024 ': 'reconnection failed'}  # disconnected too long. rescheduling job


def termination_code(logfile):
    """get return value of last run and check whether it was non-zero
    (return False if non-zero)"""
    state = None
    date  = None

    termination_match = None
    with open(logfile) as log:
        for line in log:
            state_code = line[:4]

            if state_code in STATES:
                state = STATES.get(state_code)
                date = RE_DATE.match(line).group(1)

            m = RE_RETURN.search(line)
            if m:
                termination_match = m
    try:
        return_type = termination_match.group(1)
        return_value =  int(termination_match.group(2))
    except AttributeError:
        logger.warning('Could not match the return value code')
        return_type = None
        return_value = None

    return state, date, return_type, return_value



def main(logfiles, show_all=False):
    count_failed = 0
    count_noterm = 0
    for logfile in logfiles:
        try:
            state, date, return_type, return_value = termination_code(logfile)
        except BaseException as err:
            err.args += ("At %r" % logfile,)
            raise
        if state:
            if state.startswith('terminated'):
                if return_value != 0:
                    print("Termination with error at %s: %s %2d : %s" % \
                                (date, return_type, return_value, logfile))
                    count_failed += 1
                elif show_all:
                    print("OK (%s): %s" % (date, logfile))
            else:
                print("Not terminated (%s at %s): %s" % (state, date, logfile))
                count_noterm += 1
        else:
            logger.warning("Invalid log file: %s", logfile)

    print("%d failed, %d not terminated (total: %d) " % (count_failed,
                                                         count_noterm,
                                                         len(logfiles)))


if __name__=='__main__':
    logging.basicConfig(format="%(levelname)s:%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('logfiles', nargs='+')
    parser.add_argument('-a', '--show-all', action='store_true')
    args = parser.parse_args()
    main(**vars(args))

