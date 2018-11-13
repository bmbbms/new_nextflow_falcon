import argparse
import os
import sys

import io_io
import logging
LOG = logging.getLogger()

import bash
import functional
import glob
import re

def get_tracks(db_fn):
    db_dirname, db_basename = os.path.split(db_fn)
    dbname = os.path.splitext(db_basename)[0]
    fns = glob.glob('{}/.{}.*.anno'.format(db_dirname, dbname))
    re_anno = re.compile(r'\.{}\.([^\.]+)\.anno'.format(dbname))
    tracks = [re_anno.search(fn).group(1) for fn in fns]
    return tracks

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

def script_HPC_daligner(daligner_opt, db, length_cutoff_fn, tracks, prefix):
    params = dict()
    masks = ' '.join('-m{}'.format(track) for track in tracks)
    params.update(locals())
    symlink(length_cutoff_fn, 'CUTOFF')
    return """
#LB=$(cat db_block_count)
CUTOFF=$(cat CUTOFF)
rm -f daligner-jobs.*
echo "SMRT_PYTHON_PATH_PREPEND=$SMRT_PYTHON_PATH_PREPEND"
echo "PATH=$PATH"
which HPC.daligner
HPC.daligner -P. {daligner_opt} {masks} -H$CUTOFF -f{prefix} {db} >| run_jobs.sh
    """.format(**params)



def daligner_split(daligner_opt, db_fn, length_cutoff_fn):
    db = os.path.splitext(db_fn)[0]
    dbname = os.path.basename(db)

    tracks = get_tracks(db_fn)

    script = ''.join([
        script_HPC_daligner(daligner_opt, db, length_cutoff_fn, tracks, prefix='daligner-jobs'),
    ])
    script_fn = 'split_db.sh'
    with open(script_fn, 'w') as ofs:
        exe = bash.write_sub_script(ofs, script)
    io_io.syscall('bash -vex {}'.format(script_fn))

    # We now have files like daligner-jobs.01.OVL
    # We need to parse that one. (We ignore the others.)
    lines = open('daligner-jobs.01.OVL').readlines()

    preads_aln = True if dbname == 'preads' else False
    xformer = functional.get_script_xformer(preads_aln)
    LOG.debug('preads_aln={!r} (True => use daligner_p)'.format(preads_aln))

    scripts = list()
    for line in lines:
        if line.startswith('#'):
            continue
        if not line.strip():
            continue
        line = xformer(line) # Use daligner_p for preads.
        scripts.append(line)
    """
    Special case:
        # Daligner jobs (1)
        daligner raw_reads raw_reads && mv raw_reads.raw_reads.las raw_reads.las
    In that case, the "block" name is empty. (See functional.py)
    We will rename the file. (LAmerge on a single input is a no-op, which is fine.)
    """
    if len(scripts) == 1:
        script = scripts[0]
        re_script = re.compile(r'(mv\b.*\S+\s+)(\S+)$') # no trailing newline, for now
        mo = re_script.search(script)
        if not mo:
            msg = 'Only 1 line in daligner-jobs.01.OVL, but\n {!r} did not match\n {!r}.'.format(
                re_script.pattern, script)
            LOG.warning(msg)
        else:
            new_script = re_script.sub(r'\1{dbname}.1.{dbname}.1.las'.format(dbname=dbname), script, 1)
            msg = 'Only 1 line in daligner-jobs.01.OVL:\n {!r} matches\n {!r}. Replacing with\n {!r}.'.format(
                re_script.pattern, script, new_script)
            LOG.warning(msg)
            scripts = [new_script]

    for i, script in enumerate(scripts):
        LAcheck = 'LAcheck -vS {} *.las'.format(db)
        script += '\n' + LAcheck + '\n'
        scripts[i] = "set -vex\n" +script

    for i, script in enumerate(scripts):
        job_id = 'j_{:04d}'.format(i)
        script_dir = os.path.join('.', 'daligner-scripts', job_id)
        script_fn = os.path.join(script_dir, 'run_daligner.sh')
        io_io.mkdirs(script_dir)
        with open(script_fn, 'w') as stream:
            stream.write(script)




def parse_args(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--daligner-opt', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )

    parser.add_argument(
        '--db-fn', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )

    parser.add_argument(
        '--length-cutoff', required=True,
        help='ignored for now, but non-zero will mean "No more than this."',
    )


    args = parser.parse_args(argv[1:])
    return args


def main(argv=sys.argv):
    args = parse_args(argv)
    daligner_split(args.daligner_opt,args.db_fn,args.length_cutoff)


if __name__ == "__main__":
    main()