#!/usr/bin/python2.7

"""Generate a description file for `condor_submit`"""

# TODO:
#   - define an Error class
#   - 
#   - Use a config/template file storing the prefered defaults
#   - Use the same short options as `condor_qsub`
#
# NOTE: Possible sources of bugs in /usr/local/bin/condior-submit.sh:
#       - variable NBQUEUE defined and not used.
#       - niceuser written instead of nice_user
#       - "Memory" in (Memory > 1024) is deprecated. Use TARGET.Memory instead

import os
import os.path
import sys
import argparse
from distutils.spawn import find_executable
from datetime import datetime


# Executable: first, Queue: last.
# (short option, description name, help text)
# TODO: add default columns.
ORDERED_PARAMS = (
    ('-u',   'universe'             ,None),
    ('-o',   'output'               ,None), # can contain {time}, {dir}, {base}
    ('-i',   'input'                ,None), # idem
    ('-e',   'error'                ,None), # idem
    ('-l',   'log'                  ,None), # idem
    ('-a',   'arguments'            ,None),
    ('-c',   'request_cpus'         ,None),
    ('-m',   'request_memory'       ,None),
    ('-g',   'getenv'               ,None),
    ('-id',  'initialdir'           ,None),
    ('-stf', 'should_transfer_files',None),
    ('-ro',  'run_as_owner'         ,None),
    ('-rq',  'requirements'         ,None),
    ('-nu',  'notify_user'          ,
        "email address for notification [$USER@biologie.ens.fr]"),
    ('-n',   'notification'         ,None),
    ('-ni',  'niceuser'             ,None),
    ('-p',   'priority'             ,None),
    ('-ra',  'rank'                 ,None),
    ('-cl',   'concurrency_limits'  ,None),
    ('-q',   'queue'                ,None)
    )
# TODO: add equivalent options in capital letters to read from file.

# use the Condor macros $(Cluster) and $(Process) to automatically name output
# files. $(Cluster) : the submission ID (one per submission file)
#        $(Process) : the process ID    (one per block in the file)
TEMPLATE = '{dir}/{base}_$(Cluster)-$(Process)'

PREFERED_PARAMS = {
    'output'                : [TEMPLATE + '.stdout'],
    'error'                 : [TEMPLATE + '.stderr'],
    'log'                   : [TEMPLATE + '.log'   ],
    #'notification'          : ['Always'],  # Condor default: Never
    'notify_user'           : [os.environ['USER'] + '@biologie.ens.fr'],
    'request_memory'        : ['1G'], # TODO: $(ncores) * 1024
    'getenv'                : [True],
    'should_transfer_files' : ['NO'],
    'run_as_owner'          : [True], # Condor default: True (Unix) / False (Windows)
    'concurrency_limits'    : [os.environ['USER'] + ':34']
    #'requirements'          : ['(TARGET.memory > 1024)'] # that didn't work for me
    #'Initialdir'           : os.path.abspath(outbase),
    #'universe'             : "vanilla",
    #'environment'          :
    #'priority'             : 0,
    #'log_xml'              :,
    #'input'                : $INPUT,
    #'arguments'            : $ARGS,
    #'request_cpus'         : 1,
    #'nice_user'            : False,
    #'rank'                 : kflops+1000*Memory,
    #'queue'                : 1
    }

PREFERED_PARAMS_REPR = "\n".join('    %-21s: %s' % (p, PREFERED_PARAMS[p])
                                    for p in ORDERED_PARAMS
                                    if PREFERED_PARAMS.get(p))

EPILOG="""
DETAILS:
Argument formatting:
    any argument can contain python formatting elements that will be converted:
      {time} : formatted time string. Use --timefmt to change format.
      {dir}  : dirname of description file. '.' if stdout is used.
      {base} : basename of the description file.
               'condorjob_{time}' if stdout is used.

Other Condor Arguments:
    Any other needed argument for Condor can be specified as the following:
    --{argumentname} {argumentvalue} [...].
    See `man condor_submit` for available arguments

Default Arguments:
    Unless `--condor-defaults` is used, this script has its own defaults:
    %s

Examples:
    ~/scripts/condor_descript.py subset6-7_runcodemlsh.condor.txt $HOME/scripts/codeml/run_codeml_separatedir.sh -a ENSGT00790000122969.subset{6,7}
""" % PREFERED_PARAMS_REPR


def generate_description(description, executable, dir=None, base=None,
                         condor_defaults=False, template=None,
                         timefmt='%Y%m%d-%Hh%Mm%S',
                         **user_params):
    """
    - description    : filehandle or string;
    - executable     : the mandatory argument;
    - timefmt        : format specification to replace {time} in arguments [%Y%m%d-%Hh%Mm%S]
    - condor_defaults: wether to use condor defaults instead of PREFERED_DEFAULTS
    - user_params    : params for a condor description file:
         universe              
         output                
         input                 
         error                 
         log                   
         arguments             
         request_cpus          
         request_memory        
         getenv                
         initialdir            
         should_transfer_files 
         run_as_owner          
         requirements          
         notify_user           
         notification          
         niceuser              
         priority              
         rank                  
         queue                 """
    
    generate_time = datetime.now().strftime(timefmt)

    if description == sys.stdout:
        outdir = os.path.abspath(os.path.curdir)
        outbase = "condorjob_%s" % generate_time
    else:
        outdesc = description.name if isinstance(description, file) else description
        outdir, outfile = os.path.split(outdesc)
        if not outdir: outdir = '.'
        #print "outdir: %s" % outdir, "outfile %s" % outfile
        outbase, _ = os.path.splitext(outfile)
    
    # override automatic outdir and outbase values by user-provided values.
    if dir is not None: outdir = dir.rstrip('/')
    if base is not None: outbase = base

    # Load prefered parameters if needed
    params = {}
    if not condor_defaults:
        params.update(**PREFERED_PARAMS) # TODO: move this part outside, and
        # add the defaults in the argparse.ArgumentParser
    
    # Uses user-defined template to name the output, error and log files:
    if template:
        params['output'] = [t + '.stdout' for t in template]
        params['error']  = [t + '.stderr' for t in template]
        params['log']    = [t + '.log'    for t in template]

    # replace defaults by user-specified values
    for p,v in user_params.items():
        if v is None:
            user_params.pop(p)
    params.update(**user_params)

    # Update parameters (correct type str to list, format strings)
        #if not (isinstance(v, list) or isinstance(v, tuple)):
        #    v = [v]
    for p,v in params.iteritems():
        for i, value_item in enumerate(v):
            try: 
                v[i] = value_item.format(dir=outdir, base=outbase, time=generate_time)
            except AttributeError:
                # This is not a string, do not format
                pass
            except IndexError:
                print >>sys.stderr, "Error with value:", v[i]
                print >>sys.stderr, "The only formatting characters allowed "\
                        "are {dir}, {base} and {time}. To escape curly braces"\
                        ", use {{ and }}."
                sys.exit(1)

    get_param = params.get
    ordered_params_list = [p for _,p,_ in ORDERED_PARAMS if get_param(p)]
    # Add unknown arguments
    ordered_params_list.extend(p for p in params if p not in ordered_params_list)
    single_params = [p for p in ordered_params_list if len(params[p]) == 1]
    single_params_set = set(single_params)
    perblock_params = [p for p in ordered_params_list if p not in single_params_set]

    # TODO:
    # Add extra params not known in ordered_params.

    # Open output description file.  Because both str and unicode are
    # subclasses of basestring.
    OUT = description if isinstance(description, file) else open(outdesc, 'w')
    
    executable_path = find_executable(executable)
    if not executable_path:
        print >>sys.stderr, ("Executable %r not in PATH. Please specify the "
                             "absolute or relative path") % executable
        sys.exit(1)

    OUT.write("executable = %s\n" % executable_path)
    for k in single_params:
        OUT.write("%s = %s\n" % (k, params.pop(k)[0]))
    
    if not perblock_params: # There is only one block
        OUT.write("Queue\n")
    else:
        OUT.write("\n")

        # Determine number of jobs, and check if consistent across arguments
        njobs = len(params[perblock_params[0]])
        if not all(njobs == len(params[p]) for p in perblock_params):
            print >>sys.stderr, ("Not the same number of arguments for each argument"
                                 ". Must be 1 or the same anywhere")
            sys.exit(1)

        for i in range(njobs):
            block = ''
            for param in perblock_params:
                value = get_param(param)
                if value:
                    block += "%s = %s\n" % (param, str(value[i]))
            
            block += "Queue\n\n"
            OUT.write(block)

    if OUT != sys.stdout:
        OUT.close()


def parse_unknown_args(uargs):
    """uargs: a list of strings from the command line.
    parse using the rule nargs='+' """
    uargdict = {}
    if not uargs:
        return uargdict

    opt = ''
    values = []
    while uargs:
        term = uargs.pop(0)
        if term.startswith('--') or term.startswith('-'):
            if (not values) and opt:
                print >>sys.stderr, ("Invalid option. At least one value needed "
                                     "for %s") % opt
                sys.exit(1)
            if not uargs:
                print >>sys.stderr, ("Invalid option. At least one value needed "
                                     "for %s") % term
                sys.exit(1)

            opt=term
            values = uargdict.setdefault(opt.lstrip('-'), [])
        else:
            values.append(term)
        
    return uargdict


# TODO: Use csv module
def parse_fromfile(filename):
    """Read condor arguments from space delimited table
    The first line must contain the names of the arguments"""
    with open(filename) as IN:
        try:
            argnames = IN.next().rstrip().split('\t')
        except StopIteration:
            print >>sys.stderr, "File %s is empty" % filename
            sys.exit(1)
        args_fromfile = {arg:[] for arg in argnames}
        for line in IN:
            args = line.rstrip().split('\t')
            for argname, arg in zip(argnames, args):
                args_fromfile[argname].append(arg)
    return args_fromfile


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     epilog=EPILOG,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    #g1 = parser.add_argument_group()
    condor_arg_group = parser.add_argument_group('Condor description arguments')
    # TODO: nicer-looking printing help for the condor args

    aa = parser.add_argument
    aac = condor_arg_group.add_argument

    #aa('-d', '--description','--desc', type=argparse.FileType('w'), default=sys.stdout,
    aa('description', nargs='?', type=argparse.FileType('w'), default=sys.stdout,
       help='File name to write description in. Optional [stdout].')
    aa('--dir',
       help="string used to format arguments containing '{dir}'. It uses "\
            "`dirname description file` by default")
    aa('--base',
       help="string used to format arguments containing '{base}'. It uses "\
            "`basename description file` (without extensions) by default.")
    aa('--template', nargs='+', # default=TEMPLATE,
       help="Name of output, error, and log files, without the extensions ("\
            ".stdout, .stderr, .log respectively)")
    aa('--fromfile',
       help='Take arguments from columns of a tabulated file. The '\
            'first line must contain arguments names (condor names or also '\
            'options from this script). These values will be overriden by ' \
            'commandline options, with a warning.')
    #aa('--submit', action='store_true',
    #   help='Directly submit job to the cluster')
    aa('--condor-defaults', action='store_true',
       help='Whether to use condor default arguments (not this script defaults).')
    aa('--timefmt', default='%Y%m%d-%Hh%Mm%S',
       help='time formatting (time of execution of this script)')
    
    aac('executable')
    for shortopt, longopt, hlp in ORDERED_PARAMS:
        aac(shortopt, '--' + longopt, nargs='+', help=hlp)

    args, uargs = parser.parse_known_args()
    # uargs contains unknown args. When you need to add arguments for condor
    # not defined in this script.
    dictargs = vars(args)
    dictargs.update(parse_unknown_args(uargs))
    #print dictargs
    if args.fromfile:
        args_fromfile = parse_fromfile(dictargs.pop('fromfile'))
        for argname, arg in args_fromfile.iteritems():
            if dictargs.get(argname) is not None:
                print >>sys.stderr, ("Warning: argument '%s' from file will be"
                                     " overriden by commandline")
            else:
                dictargs[argname] = arg

    generate_description(**dictargs)

