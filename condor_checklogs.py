#!/usr/bin/env python2.7


"""Output logfiles indicating an error in the last run."""

# USAGE: ./condor_logs_check.py *condor.log

from __future__ import print_function


import re
import argparse
import logging
import datetime as dt
logger = logging.getLogger(__name__)

TIME_FORMAT = '%m/%d %H:%M:%S'

RE_RETURN = re.compile(r'\(([^()]+) (\d+)\)')
RE_DATE = re.compile(r'0\d\d \([0-9.]+\) (\d+/\d+ \d+:\d+:\d+) ')
#RE_SUBMITTED = re.compile(r'000 ')
#RE_STARTED = re.compile(r'001 ')
#RE_TERM = re.compile(r'005 ')
RE_MEM = re.compile(r'^\s*Memory \(MB\)\s+:\s*(\d+)\s+(\d+)\s+(\d+)$')
RE_MEMUPDATE = re.compile(r'^\s*(\d+)\s+-\s+MemoryUsage of job ([A-Za-z]+)$')

STATES = {'000 ': 'submitted',
          '001 ': 'started',
          '004 ': 'evicted',
          '005 ': 'terminated',
          '006 ': 'Image size updated',
          '009 ': 'aborted',
          '022 ': 'disconnected',  # attempting to reconnect
          '024 ': 'reconnection failed'}  # disconnected too long. rescheduling job

ended = set(('evicted', 'terminated', 'aborted', 'disconnected', 'reconnection failed'))
running = set(('submitted', 'started', 'Image size updated'))


def termination_code(logfile):
    """get return value of last run and check whether it was non-zero
    (return False if non-zero)"""
    state = None
    date  = None

    termination_match = None
    memory_match = None
    memupdates = []  # All memory values at logged timepoints.
    with open(logfile) as log:
        for line in log:
            state_code = line[:4]

            if state_code in STATES:
                state = STATES.get(state_code)
                date = RE_DATE.match(line).group(1)

            if state == 'terminated':
                m = RE_RETURN.search(line)
                if m:
                    termination_match = m
                else:
                    m = RE_MEM.match(line)
                    if m:
                        memory_match = m
                memupdates = []
            elif state == 'Image size updated':
                m = RE_MEMUPDATE.match(line)
                if m:
                    memupdates.append(int(m.group(1)))
            elif state in set(('submitted', 'started')):
                memory_match = None
                memupdates = []

    try:
        return_type = termination_match.group(1)
        return_value =  int(termination_match.group(2))
    except AttributeError:
        if state == 'terminated':
            logger.warning('Could not match the return value code')
        return_type = None
        return_value = None
    try:
        memories = tuple(int(x) for x in memory_match.groups())
    except AttributeError:
        if state == 'terminated':
            logger.warning('Could not match the memory amounts')
        elif memupdates:
            memories = (max(memupdates), None, None)
        else:
            memories = (None,)*3

    return (state, date, return_type, return_value) + memories


def main(logfiles, show_all=False, terminated_only=False, memory=False, sort=False):
    count_failed = 0
    count_noterm = 0
    used_memory = []
    outputs = []
    for logfile in logfiles:
        try:
            state, date, return_type, return_value, mem_used, _, mem_alloc = \
                    termination_code(logfile)
        except BaseException as err:
            err.args += ("At %r" % logfile,)
            raise
        msg = None
        if state:
            if state.startswith('terminated'):
                if memory:
                    used_memory.append(mem_used)
                    if mem_used > mem_alloc:
                        msg = 'Exceeded allocated memory! %d > %d (MB): %s' % (
                                mem_used, mem_alloc, logfile)
                        count_failed += 1
                    else:
                        msg = 'OK: memory used %d <= %d memory allocated (MB): %s' % (
                                mem_used, mem_alloc, logfile)
                else:
                    if return_value != 0:
                        msg = "Termination with error at %s: %s %2d : %s" % (
                                date, return_type, return_value, logfile)
                        count_failed += 1
                    elif show_all or terminated_only:
                        msg = "OK (%s): %s" % (date, logfile)
            elif state in ended:
                if memory:
                    if mem_used is not None:
                        msg = "Condor termination (max memory = %s (MB)): %s" % (
                                mem_used, logfile)
                        used_memory.append(mem_used)
                else:
                    msg = "Condor termination (%s at %s): %s" % (state, date, logfile)
                count_noterm += 1
            else:
                if not terminated_only:
                    if memory:
                        msg = "Running (max_memory %s (MB)): %s" % (mem_used, logfile)
                        if mem_used is not None:
                            used_memory.append(mem_used)
                    else:
                        msg = "Not terminated (%s at %s): %s" % (state, date, logfile)
                count_noterm += 1
        else:
            logger.warning("Invalid log file: %s", logfile)

        if msg:
            if sort:
                outputs.append((dt.datetime.strptime(date, TIME_FORMAT), msg))
            else:
                print(msg)
    
    if sort:
        for _, msg in sorted(outputs, key=lambda x: x[0]):
            print(msg)

    if memory:
        print("%d exceeded, %d not terminated (max: %d MB, total: %d)"
              % (count_failed, count_noterm, max(used_memory), len(logfiles)))
    else:
        print("%d failed, %d not terminated (total: %d) " % (count_failed,
                                                             count_noterm,
                                                             len(logfiles)))


if __name__=='__main__':
    logging.basicConfig(format="%(levelname)s:%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('logfiles', nargs='+')
    show_group = parser.add_mutually_exclusive_group()
    show_group.add_argument('-a', '--show-all', action='store_true')
    show_group.add_argument('-t', '--terminated-only', action='store_true',
                        help='Only print the terminated jobs.')
    parser.add_argument('-m', '--memory', action='store_true',
                        help='Report jobs that exceeded allocated memory.')
    parser.add_argument('-s', '--sort', action='store_true',
                        help='Sort by time')
    args = parser.parse_args()
    main(**vars(args))

