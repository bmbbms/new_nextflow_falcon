import argparse
import collections
import os
import re
import sys

import io_io


def merge_split( las_paths_fn,dbname="raw_reads"):

    las_paths = io_io.deserialize(las_paths_fn)

    re_las_pair = re.compile(r'{db}\.(\d+)\.{db}\.(\d+)\.las$'.format(db=dbname))
    las_map = collections.defaultdict(list)
    for path in las_paths:
        mo = re_las_pair.search(path)
        if not mo:
            msg = '{!r} does not match regex {!r}'.format(
                path, re_las_pair.pattern)
            raise Exception(msg)
        a, b = int(mo.group(1)), int(mo.group(2))
        las_map[a].append(path)

    for i, block in enumerate(las_map):
        job_id = 'm_{:05d}'.format(i)

        # Write the las files for this job.
        input_dir = os.path.join('merge-scripts', job_id)
        las_paths_fn = os.path.join('.', input_dir, 'las-paths.json')
        io_io.mkdirs(input_dir)
        las_paths = las_map[block]
        io_io.serialize(las_paths_fn, las_paths)

def parse_args(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--las-fn', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )


    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    args = parse_args(argv)
    merge_split(args.las_fn)
    # rep_combine("a.db","test.json",3,"")


if __name__ == "__main__":
    main()