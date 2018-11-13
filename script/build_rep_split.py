import argparse
import logging
import sys

import io_io
import os
import re
LOG = logging.getLogger()
WAIT = 20


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

def rep_split( las_paths_fn,  group_size, coverage_limit):
    """For foo.db, HPC.REPmask would produce rep-jobs.05.MASK lines like this:

    # REPmask jobs (n)
    REPmask -v -c30 -nrep1 foo foo.R1.@1-3
    REPmask -v -c30 -nrep1 foo foo.R1.@4-6
    ...

    (That's for level R1.)
    We will do one block at-a-time, for simplicity.
    """


    las_paths = io_io.deserialize(las_paths_fn)

    scripts = list()
    for i, las_fn in enumerate(las_paths):
        las_files = las_fn # one at-a-time
        script_lines = [
            'set -vex\n',
            #'LAcheck {} {}\n'.format(db, las_files),
            'REPmask -v -c{} -nrep{} {} {}\n'.format(
                coverage_limit, group_size, "raw_reads", las_files),
            '#rm -f {}\n'.format(las_files),
        ]
        scripts.append(''.join(script_lines))

    for i, script in enumerate(scripts):
        job_id = 'rep_{:03d}'.format(i)
        script_dir = os.path.join('.', 'rep-scripts', job_id)
        script_fn = os.path.join(script_dir, 'run_REPmask.sh')
        io_io.mkdirs(script_dir)
        with open(script_fn, 'w') as stream:
            stream.write('{}\n'.format(script))


def parse_args(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--las-json', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )


    parser.add_argument(
        '--group-size', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )

    parser.add_argument(
        '--coverage', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )

    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    args = parse_args(argv)
    rep_split(args.las_json,args.group_size,args.coverage)


if __name__ == "__main__":
    main()