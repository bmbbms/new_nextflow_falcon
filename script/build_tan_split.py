from __future__ import absolute_import

import argparse
import logging
import os
import sys
import re
from io_io import yield_validated_fns
import io_io
import functional
import bash
LOG = logging.getLogger()
WAIT = 20 # seconds to wait for file to exist


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


def script_HPC_TANmask(tanmask_opt, db, prefix):
    assert prefix and '/' not in prefix
    params = dict()
    params.update(locals())
    return """
rm -f {prefix}.*
rm -f .{db}.*.tan.anno
rm -f .{db}.*.tan.data
echo "SMRT_PYTHON_PATH_PREPEND=$SMRT_PYTHON_PATH_PREPEND"
echo "PATH=$PATH"
which HPC.TANmask
HPC.TANmask -P. {tanmask_opt} -v -f{prefix} {db}
    """.format(**params)

def tan_split(tanmask_opt, db_fn, uows_fn, bash_template_fn):
    with open(bash_template_fn, 'w') as stream:
        stream.write("python -m falcon_kit.mains.dazzler --config={input.config} --db={input.db}  tan-split --split={output.split} --bash-template={output.bash_template}")
    # TANmask would put track-files in the DB-directory, not '.',
    # so we need to symlink everything first.
    db = symlink_db(db_fn)

    script = ''.join([
        script_HPC_TANmask(tanmask_opt, db, prefix='tan-jobs'),
    ])
    script_fn = 'split_db.sh'
    with open(script_fn, 'w') as ofs:
        exe = bash.write_sub_script(ofs, script)
    io_io.syscall('bash -vex {}'.format(script_fn))

    # We now have files like tan-jobs.01.OVL
    # We need to parse that one. (We ignore the others.)
    lines = open('tan-jobs.01.OVL').readlines()

    re_block = re.compile(r'{}(\.\d+|)'.format(db))

    def get_blocks(line):
        """Return ['.1', '.2', ...]
        """
        return [mo.group(1) for mo in re_block.finditer(line)]

    scripts = list()
    for line in lines:
        if line.startswith('#'):
            continue
        if not line.strip():
            continue
        blocks = get_blocks(line)
        assert blocks, 'No blocks found in {!r} from {!r}'.format(line, 'tan-jobs.01.OVL')
        las_files = ' '.join('TAN.{db}{block}.las'.format(db=db, block=block) for block in blocks)
        script_lines = [
            line,
            'LAcheck {} {}\n'.format(db, las_files),
            'TANmask {} {}\n'.format(db, las_files),
            'rm -f {}\n'.format(las_files),
        ]
        if [''] == blocks:
            # special case -- If we have only 1 block, then HPC.TANmask fails to use the block-number.
            # However, if there are multiple blocks, it is still possible for a single line to have
            # only 1 block. So we look for a solitary block that is '', and we symlink the .las to pretend
            # that it was named properly in the first place.
            script_lines.append('mv .{db}.tan.data .{db}.1.tan.data\n'.format(db=db))
            script_lines.append('mv .{db}.tan.anno .{db}.1.tan.anno\n'.format(db=db))
        scripts.append(''.join(script_lines))
    db_dir = os.path.dirname(db_fn)

    for i, script in enumerate(scripts):
        bash_script = """
db_dir={db_dir}
ln -sf ${{db_dir}}/.{db_prefix}.bps .
ln -sf ${{db_dir}}/.{db_prefix}.idx .
ln -sf ${{db_dir}}/{db_prefix}.db .
ln -sf ${{db_dir}}/.{db_prefix}.dust.anno .
ln -sf ${{db_dir}}/.{db_prefix}.dust.data .
{script}
""".format(db_dir=db_dir,db_prefix="raw_reads",script=script)
        job_id = 'tan_{:03d}'.format(i  )
        script_dir = os.path.join('.', 'tan-scripts', job_id)
        script_fn = os.path.join(script_dir, 'run_datander.sh')
        io_io.mkdirs(script_dir)
        with open(script_fn, 'w') as stream:
            stream.write('{}\n'.format(bash_script))





def cmd_tan_split(args):

    tan_split(args.TANmask_opt, args.db_fn, args.split_fn, args.bash_template_fn)



def add_tan_split_arguments(parser):
    parser.add_argument(
        '--split-fn', default='tan-mask-uows.json',
        help='output. Units-of-work from HPC.TANmask, for datander.',
    )
    parser.add_argument(
        '--bash-template-fn', default='bash-template.sh',
        help='output. Script to apply later.',
    )
    parser.add_argument(
        '--TANmask-opt', default='',
        help='tanmask argument',
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

    help_tan_split = 'generate units-of-work for datander, via HPC.TANmask'

    subparsers = parser.add_subparsers(help='sub-command help')


    parser_tan_split = subparsers.add_parser('tan-split',
                                             formatter_class=HelpF,
                                             description=help_tan_split,
                                             epilog='',
                                             help=help_tan_split)
    add_tan_split_arguments(parser_tan_split)
    parser_tan_split.set_defaults(func=cmd_tan_split)


    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    args = parse_args(argv)
    print(args.log_level)
    args.func(args)


if __name__ == '__main__':  # pragma: no cover
    main()
