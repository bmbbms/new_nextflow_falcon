from __future__ import absolute_import

import argparse
import logging
import os
import sys
from io_io import yield_validated_fns
import io_io
import functional
import bash
LOG = logging.getLogger()
WAIT = 20 # seconds to wait for file to exist


class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass


def filter_DBsplit_option(opt):
    """We want -a by default, but if we see --no-a[ll], we will not add -a.
    """
    flags = opt.split()
    if '-x' not in opt:
        flags.append('-x70')  # daligner belches on any read < kmer length
    return ' '.join(flags)


def script_build_db(input_fofn_fn, db,pa_DBdust_option,fasta_filter_option,fasta_filter_py):
    """
    db (e.g. 'raw_reads.db') will be output into CWD, should not already exist.
    'dust' track will also be generated.
    """
    params = dict()
    if not pa_DBdust_option:
        pa_DBdust_option = ""
    if not fasta_filter_option:
        fasta_filter_option="pass"

    try:
        cat_fasta = functional.choose_cat_fasta(open(input_fofn_fn).read())
    except Exception:
        LOG.exception('Using "cat" by default.')
        cat_fasta = 'cat '
    DBdust = 'DBdust {} {}'.format(pa_DBdust_option, db)
    params.update(locals())
    script = """  
echo "PBFALCON_ERRFILE=$PBFALCON_ERRFILE"
set -o pipefail
rm -f {db}.db .{db}.* # in case of re-run
#fc_fasta2fasta < {input_fofn_fn} >| fc.fofn
while read fn; do  {cat_fasta} ${{fn}} | python {fasta_filter_py} {fasta_filter_option} - | fasta2DB -v {db} -i${{fn##*/}}; done < {input_fofn_fn}
#cat fc.fofn | xargs rm -f
{DBdust}
""".format(**params)
    return script


def script_length_cutoff( db,seed_coverage,genome_size,length_cutoff,length_cutoff_fn='length_cutoff'):
    params = dict()

    if int(length_cutoff) < 0:
        if not seed_coverage or not genome_size:
            LOG.exception("must have values seed_coverage and genome_size if length_cutoff = -1")
        bash_cutoff = '$(python2.7 -m calc_cutoff --coverage {} {} <(DBstats -b1 {}))'.format(
            seed_coverage, genome_size, db)
    else:
        bash_cutoff = '{}'.format(length_cutoff)
    params.update(locals())
    return """
CUTOFF={bash_cutoff}
echo -n $CUTOFF >| {length_cutoff_fn}
""".format(**params)


def script_DBsplit( db,DBsplit_opt):
    params = dict()
    params.update(locals())
    DBsplit_opt = filter_DBsplit_option(DBsplit_opt)
    params.update(locals())
    return """
DBsplit -f {DBsplit_opt} {db}
LB=$(cat {db}.db | LD_LIBRARY_PATH= awk '$1 == "blocks" {{print $3}}')
echo -n $LB >| db_block_count
""".format(**params)


def cmd_build(args):
    # ours = get_ours(args.config_fn, args.db_fn)
    LOG.info('Building rdb from {!r}, to write {!r}'.format(args.input_fofn_fn, args.db_fn))
    db = os.path.splitext(args.db_fn)[0]

    # First, fix-up FOFN for thisdir.
    my_input_fofn_fn = 'my.' + os.path.basename(args.input_fofn_fn)
    with open(my_input_fofn_fn, 'w') as stream:
        for fn in yield_validated_fns(args.input_fofn_fn):
            stream.write(fn)
            stream.write('\n')
    script = ''.join([
        script_build_db(my_input_fofn_fn, db,args.pa_DBdust_option,args.fasta_filter_option,args.fasta_filter_py),
        script_DBsplit(db, args.DBsplit_opt),
        script_length_cutoff(db, args.seed_coverage,args.genome_size,args.length_cutoff,args.length_cutoff_fn),
    ])
    script_fn = 'build_db.sh'
    with open(script_fn, 'w') as ofs:
        bash.write_sub_script(ofs, script)



def add_build_arguments(parser):
    parser.add_argument(
        '--input-fofn-fn', required=True,
        help='input. User-provided file of fasta filename. Relative paths are relative to directory of FOFN.',
    )
    parser.add_argument(
        '--length-cutoff-fn', required=True,
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )
    parser.add_argument(
        '--pa-DBdust-option', required=True,
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )

    parser.add_argument(
        '--fasta-filter-option',
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )

    parser.add_argument(
        '--DBsplit-opt', required=True,
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )
    parser.add_argument(
        '--seed-coverage',
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )
    parser.add_argument(
        '--genome-size',
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )
    parser.add_argument(
        '--length-cutoff', required=True,
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )
    parser.add_argument(
        '--fasta-filter-py', required=True,
        help='output. Simple file of a single integer, either calculated or specified by --user-length-cutoff.'
    )


options_note = """

For raw_reads.db, we also look for the following config keys:

- pa_DBsplit_option
- pa_HPCdaligner_option
- pa_HPCTANmask_option
- pa_daligner_option
- length_cutoff: -1 => calculate based on "genome_size" and "seed_coverage" config.
- seed_coverage
- genome_size

For preads.db, these are named:

- ovlp_DBsplit_option
- ovlp_HPCdaligner_option
- ovlp_daligner_option
- length_cutoff_pr
"""


def parse_args(argv):
    description = 'Basic daligner steps: build; split into units-of-work; combine results and prepare for next step.'
    epilog = 'These tasks perform the split/apply/combine strategy (of which map/reduce is a special case).' + options_note
    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=HelpF,
    )
    parser.add_argument(
        '--log-level', default='INFO',
        help='Python logging level.',
    )
    parser.add_argument(
        '--nproc', type=int, default=0,
        help='ignored for now, but non-zero will mean "No more than this."',
    )

    parser.add_argument(
        '--db-fn', default='raw_reads.db',
        help='Input or Output. Dazzler DB. (Dot-files are implicit.)',
    )

    help_build = 'build Dazzler DB for raw_reads; calculate length-cutoff for HGAP seed reads; split Dazzler DB into blocks; run DBdust to mask low-complexity regions'

    subparsers = parser.add_subparsers(help='sub-command help')

    parser_build = subparsers.add_parser('build',
                                         formatter_class=HelpF,
                                         description=help_build,
                                         help=help_build)
    add_build_arguments(parser_build)
    parser_build.set_defaults(func=cmd_build)
    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    args = parse_args(argv)
    print(args.log_level)
    args.func(args)


if __name__ == '__main__':  # pragma: no cover
    main()
