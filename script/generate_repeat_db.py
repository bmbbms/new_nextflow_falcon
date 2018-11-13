import argparse
import glob
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


def rep_combine(db_fn, gathered_fn, group_size):
    # new_db = "rep_db"
    db = symlink_db(db_fn)
    # db ="raw_reads"
    # Remove old, in case of resume.
    # io_io.syscall('rm -f .{db}.*.rep{group_size}.anno .{db}.*.rep{group_size}.data'.format(**locals()))

    # gathered = io_io.deserialize(gathered_fn)
    # gathered_dn = os.path.dirname(gathered_fn)
    with open (gathered_fn,"r") as f:
        data = f.readlines()[0][1:-1]
        new_data = data.replace(" ","")
        gathered =new_data.split(",")
    print(len(gathered))
    # Create symlinks for all track-files.
    for job in gathered:
        job_path = os.path.dirname(job)
        if not os.path.isabs(job_path):
            LOG.info('Found relative done-file: {!r}'.format(job_path))
        annos = glob.glob('{}/.{}.*.rep{}.anno'.format(job_path, db, group_size))
        datas = glob.glob('{}/.{}.*.rep{}.data'.format(job_path, db, group_size))
        assert len(annos) == len(datas), 'Mismatched globs:\n{!r}\n{!r}'.format(annos, datas)
        for fn in annos + datas:
            symlink(fn, force=False)
    cmd = 'Catrack -vdf {} rep{}'.format(db, group_size)
    io_io.syscall(cmd)
   



def parse_args(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--gather-fn', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )

    parser.add_argument(
        '--db-fn', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )
    parser.add_argument(
        '--group-size', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )

    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    args = parse_args(argv)
    rep_combine(args.db_fn,args.gather_fn,args.group_size)
    # rep_combine("a.db","test.json",3,"")


if __name__ == "__main__":
    main()