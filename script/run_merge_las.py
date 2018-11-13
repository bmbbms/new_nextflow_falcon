import argparse
import sys

import io_io
import os
import logging
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

def ichunked(seq, chunksize):
    """Yields items from an iterator in iterable chunks.
    https://stackoverflow.com/a/1335572
    """
    from itertools import chain, islice
    it = iter(seq)
    while True:
        yield chain([it.next()], islice(it, chunksize-1))

def merge_apply(las_paths_fn, las_fn):
    """Merge the las files into one, a few at a time.
    This replaces the logic of HPC.daligner.
    """
    with open(las_fn,"r") as f:
        las_name = f.readlines()[0].strip()
    io_io.rm_force(las_name)
    print(las_name)
    #all_las_paths = rel_to(io_io.deserialize(las_paths_fn), os.path.dirname(las_paths_fn))
    all_las_paths = io_io.deserialize(las_paths_fn)

    # Create symlinks, so system calls will be shorter.
    all_syms = list()
    for fn in all_las_paths:
        symlink(fn)
        all_syms.append(os.path.basename(fn))
    curr_paths = sorted(all_syms)

    # Merge a few at-a-time.
    at_a_time = 250 # max is 252 for LAmerge
    level = 1
    while len(curr_paths) > 1:
        level += 1
        next_paths = list()
        for i, paths in enumerate(ichunked(curr_paths, at_a_time)):
            tmp_las = 'L{}.{}.las'.format(level, i+1)
            paths_arg = ' '.join(paths)
            cmd = 'LAmerge -v {} {}'.format(tmp_las, paths_arg)
            io_io.syscall(cmd)
            next_paths.append(tmp_las)
        curr_paths = next_paths

    io_io.syscall('mv -f {} {}'.format(curr_paths[0], 'keep-this'))
    io_io.syscall('mv -f  {} {}'.format('keep-this', las_name))

def parse_args(argv):
    description = 'Basic daligner steps: build; split into units-of-work; combine results and prepare for next step.'
    epilog = 'These tasks perform the split/apply/combine strategy (of which map/reduce is a special case).'
    parser = argparse.ArgumentParser(
        description=description,
        epilog=epilog
    )

    parser.add_argument(
        '--las-path', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )
    parser.add_argument(
        '--las-fn', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )








    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    logging.basicConfig(level=2)
    args = parse_args(argv)
    merge_apply(args.las_path,args.las_fn)



if __name__ == '__main__':  # pragma: no cover
    main()
