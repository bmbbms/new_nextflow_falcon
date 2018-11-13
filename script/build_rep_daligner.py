from __future__ import absolute_import

import argparse
import glob
import logging
import os
import sys
import re
import io_io
import bash
LOG = logging.getLogger()
WAIT = 20


class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass

def symlink(actual, symbolic=None, force=True):
    """Symlink into cwd, relatively.
    symbolic name is basename(actual) if not provided.
    If not force, raise when already exists and does not match.
    But ignore symlink to self.
    """
    symbolic = os.path.basename(actual) if not symbolic else symbolic
    if os.path.abspath(actual) == os.path.abspath(symbolic):
        LOG.warning('Cannot symlink {!r} as {!r}, itself.'.format(actual, symbolic))
        return
    rel = os.path.relpath(actual)
    if force:
        LOG.info('ln -sf {} {}'.format(rel, symbolic))
        if os.path.lexists(symbolic):
            if os.readlink(symbolic) == rel:
                return
            else:
                os.unlink(symbolic)
    else:
        LOG.info('ln -s {} {}'.format(rel, symbolic))
        if os.path.lexists(symbolic):
            if os.readlink(symbolic) != rel:
                msg = '{!r} already exists as {!r}, not {!r}'.format(
                        symbolic, os.readlink(symbolic), rel)
                raise Exception(msg)
            else:
                LOG.info('{!r} already points to {!r}'.format(symbolic, rel))
                return
    os.symlink(rel, symbolic)

def symlink_db(db_fn):
    """Symlink everything that could be related to this Dazzler DB.
    Exact matches will probably cause an exception in symlink().
    """
    db_dirname, db_basename = os.path.split(db_fn)
    dbname = os.path.splitext(db_basename)[0]

    fn = os.path.join(db_dirname, dbname + '.db')
    symlink(fn)

    re_suffix = re.compile(r'^\.%s(\.idx|\.bps|\.dust\.data|\.dust\.anno|\.tan\.data|\.tan\.anno|\.rep\d+\.data|\.rep\d+\.anno)$'%dbname)
    all_basenames = os.listdir(db_dirname)
    for basename in sorted(all_basenames):
        mo = re_suffix.search(basename)
        if not mo:
            continue
        fn = os.path.join(db_dirname, basename)
        if os.path.exists(fn):
            symlink(fn)
        else:
            LOG.warning('Symlink {!r} seems to be broken.'.format(fn))
    return dbname

def get_tracks(db_fn):
    db_dirname, db_basename = os.path.split(db_fn)
    dbname = os.path.splitext(db_basename)[0]
    fns = glob.glob('{}/.{}.*.anno'.format(db_dirname, dbname))
    re_anno = re.compile(r'\.{}\.([^\.]+)\.anno'.format(dbname))
    tracks = [re_anno.search(fn).group(1) for fn in fns]
    return tracks

def script_HPC_REPmask(REPmask_opt, db, tracks, prefix, group_size, coverage_limit):
    if group_size == 0: # TODO: Make this a no-op.
        group_size = 1
        coverage_limit = 10**9 # an arbitrary large number
    assert prefix and '/' not in prefix
    params = dict()
    params.update(locals())
    return """
rm -f {prefix}.*
rm -f .{db}.*.rep*.anno
rm -f .{db}.*.rep*.data
echo "SMRT_PYTHON_PATH_PREPEND=$SMRT_PYTHON_PATH_PREPEND"
echo "PATH=$PATH"
which HPC.REPmask
HPC.REPmask -P. -g{group_size} -c{coverage_limit} {REPmask_opt} -v -f{prefix} {db}
    """.format(**params)

def fake_rep_as_daligner_script_moved(script, dbname):
    """
    Special case:
        # Daligner jobs (1)
        daligner raw_reads raw_reads && mv raw_reads.raw_reads.las raw_reads.R1.1.las
    Well, unlike for daligner_split, here the block-number is there for this degenerate case. Good!
    """
    """
    We have db.Rn.block.las
    We want db.block.db.block.las, for now. (We will 'merge' this solo file later.)
    """
    re_script = re.compile(r'(mv\b.*\S+\s+)(\S+)$') # no trailing newline, for now
    mo = re_script.search(script)
    if not mo:
        msg = 'Only 1 line in foo-jobs.01.OVL, but\n {!r} did not match\n {!r}.'.format(
            re_script.pattern, script)
        LOG.warning(msg)
        return script
    else:
        new_script = re_script.sub(r'\1{dbname}.1.{dbname}.1.las'.format(dbname=dbname), script, 1)
        msg = 'Only 1 line in foo-jobs.01.OVL:\n {!r} matches\n {!r}. Replacing with\n {!r}.'.format(
            re_script.pattern, script, new_script)
        LOG.warning(msg)
        return new_script

def fake_rep_as_daligner_script_unmoved(script, dbname):
    """
    Typical case:
        # Daligner jobs (N)
        daligner raw_reads raw_reads && mv raw_reads.3.raw_reads.3.las raw_reads.R1.3.las
    Well, unlike for daligner_split, here the block-number is there for this degenerate case. Good!
    """
    """
    We have db.Rn.block.las
    We want db.block.db.block.las. (We will merge later.)
    """
    re_script = re.compile(r'\s*\&\&\s*mv\s+.*$') # no trailing newline, for now
    mo = re_script.search(script)
    if not mo:
        msg = 'Many lines in foo-jobs.01.OVL, but\n {!r} did not match\n {!r}.'.format(
            re_script.pattern, script)
        LOG.warning(msg)
        return script
    else:
        new_script = re_script.sub('', script, 1)
        msg = 'Many lines in foo-jobs.01.OVL:\n {!r} matches\n {!r}. Replacing with\n {!r}.'.format(
            re_script.pattern, script, new_script)
        LOG.warning(msg)
        return new_script

def _get_rep_daligner_split_scripts(REPmask_opt, db_fn, group_size, coverage_limit):
    db = os.path.splitext(db_fn)[0]
    dbname = os.path.basename(db)
    tracks = get_tracks(db_fn)

    # First, run HPC.REPmask immediately.
    script = ''.join([
        script_HPC_REPmask(REPmask_opt, db, tracks,
            prefix='rep-jobs', group_size=group_size, coverage_limit=coverage_limit),
    ])
    script_fn = 'split_db.sh'
    with open(script_fn, 'w') as ofs:
        exe = bash.write_sub_script(ofs, script)
    io_io.syscall('bash -vex {}'.format(script_fn))

    # We now have files like rep-jobs.01.OVL
    # We need to parse that one. (We ignore the others.)
    lines = open('rep-jobs.01.OVL').readlines()

    scripts = list()
    for line in lines:
        if line.startswith('#'):
            continue
        if not line.strip():
            continue
        scripts.append(line)

    if len(scripts) == 1:
        scripts = [fake_rep_as_daligner_script_moved(s, dbname) for s in scripts]
    else:
        scripts = [fake_rep_as_daligner_script_unmoved(s, dbname) for s in scripts]

    for i, script in enumerate(scripts):
        LAcheck = 'LAcheck -vS {} *.las'.format(db)
        script += '\n' + LAcheck + '\n'
        scripts[i] = "set -uex\n"+script

    return scripts


def rep_daligner_split(REPmask_opt, db_fn,  group_size, coverage_limit):
    """Similar to daligner_split(), but based on HPC.REPmask instead of HPC.daligner.
    """



    scripts = _get_rep_daligner_split_scripts(REPmask_opt, db_fn, group_size, coverage_limit)

    for i, script in enumerate(scripts):
        job_id = 'rep_{:04d}'.format(i)
        script_dir = os.path.join('.', 'rep-scripts', job_id)
        script_fn = os.path.join(script_dir, 'run_daligner.sh')
        io_io.mkdirs(script_dir)
        with open(script_fn, 'w') as stream:
            stream.write('{}\n'.format(script))


def cmd_rep_daligner_split(args):
    rep_daligner_split(
        args.REPmask_opt,  args.db_fn, args.group_size, args.coverage_limit,

    )

def add_rep_daligner_split_arguments(parser):
    parser.add_argument(
        '--REPmask-opt', required=True,
        help='Comma-separated string of keys to be subtituted into output paths for each job, if any. (Helps with snakemake and pypeflow; not needed in pbsmrtpipe, since outputs are pre-determined.)',
    )
    parser.add_argument(
        '--group-size', '-g', required=True, type=int,
        help='Number of blocks per group. This should match what was passed to HPC.REPmask. Here, it becomes part of the mask name, repN.',
    )
    parser.add_argument(
        '--coverage-limit', '-c', required=True, type=int,
        help='Coverage threshold for masking.',
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
        '--db-fn', default='raw_reads.db',
        help='Input or Output. Dazzler DB. (Dot-files are implicit.)',
    )

    help_rep_daligner_split = 'generate units-of-work for daligner, via HPC.REPmask; should be followed ' \
                              'by daligner-apply and daligner-combine, then merge-*, then rep-*'

    subparsers = parser.add_subparsers(help='sub-command help')




    parser_rep_daligner_split = subparsers.add_parser('rep-daligner-split',
                                                      formatter_class=HelpF,
                                                      description=help_rep_daligner_split,
                                                      epilog='HPC.REPmask will be passed mask flags for any mask tracks which we glob.',
                                                      help=help_rep_daligner_split)
    add_rep_daligner_split_arguments(parser_rep_daligner_split)
    parser_rep_daligner_split.set_defaults(func=cmd_rep_daligner_split)


    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    args = parse_args(argv)
    print(args.log_level)
    args.func(args)


if __name__ == '__main__':  # pragma: no cover
    main()
