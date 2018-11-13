from __future__ import absolute_import
from __future__ import division

import io
import logging

from builtins import object
import contextlib
import os
import resource
import shlex
import shutil
import subprocess as sp
import sys
import tempfile
import traceback


def mkdir(d):
    if not os.path.isdir(d):
        os.makedirs(d)


def mkdirs(*dirnames):
    for dirname in dirnames:
        if not dirname:
            continue # '' => curdir
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
            if len(dirnames) == 1:
                LOG.debug('mkdir -p "{}"'.format(dirnames[0]))
def write_nothing(*args):
    """
    To use,
      LOG = noop
    """


def write_with_pid(*args):
    msg = '[%d]%s\n' % (os.getpid(), ' '.join(args))
    sys.stderr.write(msg)


LOG = write_with_pid


def logstats():
    """This is useful 'atexit'.
    """
    LOG('maxrss:%9d' % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))


def reprarg(arg):
    """Reduce the size of repr()
    """
    if isinstance(arg, str):
        if len(arg) > 100:
            return '{}...({})'.format(arg[:100], len(arg))
    elif (isinstance(arg, set) or isinstance(arg, list)
          or isinstance(arg, tuple) or isinstance(arg, dict)):
        if len(arg) > 9:
            return '%s(%d elem)' % (type(arg).__name__, len(arg))
        else:
            return '<' + ', '.join(reprarg(a) for a in arg) + '>'
    return repr(arg)


def run_func(args):
    """Wrap multiprocessing.Pool calls.
    Usage:
        pool.imap(run_func, [func, arg0, arg1, ...])
    """
    func = args[0]
    try:
        func_name = func.__name__
    except:
        # but since it must be pickle-able, this should never happen.
        func_name = repr(func)
    args = args[1:]
    try:
        LOG('starting %s(%s)' % (func_name, ', '.join(reprarg(a) for a in args)))
        logstats()
        ret = func(*args)
        logstats()
        LOG('finished %s(%s)' % (func_name, ', '.join(reprarg(a) for a in args)))
        return ret
    except Exception:
        raise Exception(traceback.format_exc())
    except:  # KeyboardInterrupt, SystemExit
        LOG('interrupted %s(%s)' %
            (func_name, ', '.join(reprarg(a) for a in args)))
        return


def system(call, check=False):
    LOG('$(%s)' % repr(call))
    rc = os.system(call)
    msg = "Call %r returned %d." % (call, rc)
    if rc:
        LOG("WARNING: " + msg)
        if check:
            raise Exception(msg)
    else:
        LOG(msg)
    return rc


def syscall(call, nocheck=False):
    """Raise Exception in error, unless nocheck==True
    """
    rc = os.system(call)
    msg = 'Call %r returned %d.' % (call, rc)
    if rc:
        if not nocheck:
            raise Exception(msg)
    return rc

def slurplines(cmd):
    return syscall(cmd).splitlines()


def streamlines(cmd):
    """Stream stdout from cmd.
    Let stderr fall through.
    The returned reader will stop yielding when the subproc exits.
    Note: We do not detect a failure in the underlying process.
    """
    LOG('$ %s |' % cmd)
    proc = sp.Popen(shlex.split(cmd), stdout=sp.PIPE)
    return proc.stdout


class DataReaderContext(object):
    def readlines(self):
        output = self.data.strip()
        for line in output.splitlines():
            yield line

    def __enter__(self):
        pass

    def __exit__(self, *args):
        self.returncode = 0

    def __init__(self, data):
        self.data = data


class ProcessReaderContext(object):
    """Prefer this to slurplines() or streamlines().
    """

    def readlines(self):
        """Generate lines of native str.
        """
        # In py2, not unicode.
        raise NotImplementedError()

    def __enter__(self):
        LOG('{!r}'.format(self.cmd))
        self.proc = sp.Popen(shlex.split(self.cmd), stdout=sp.PIPE, universal_newlines=True)

    def __exit__(self, etype, evalue, etb):
        if etype is None:
            self.proc.wait()
        else:
            # Exception was raised in "with-block".
            # We cannot wait on proc b/c it might never finish!
            pass
        self.returncode = self.proc.returncode
        if self.returncode:
            msg = "%r <- %r" % (self.returncode, self.cmd)
            raise Exception(msg)
        del self.proc

    def __init__(self, cmd):
        self.cmd = cmd


def splitlines_iter(text):
    """This is the same as splitlines, but with a generator.
    """
    # https://stackoverflow.com/questions/3054604/iterate-over-the-lines-of-a-string
    assert isinstance(text, str)
    prevnl = -1
    while True:
        nextnl = text.find('\n', prevnl + 1)  # u'\n' would force unicode
        if nextnl < 0:
            break
        yield text[prevnl + 1:nextnl]
        prevnl = nextnl
    if (prevnl + 1) != len(text):
        yield text[prevnl + 1:]


class CapturedProcessReaderContext(ProcessReaderContext):
    def readlines(self):
        """Usage:

            cmd = 'ls -l'
            reader = CapturedProcessReaderContext(cmd)
            with reader:
                for line in reader.readlines():
                    print line

        Any exception within the 'with-block' is propagated.
        Otherwise, after all lines are read, if 'cmd' failed, Exception is raised.
        """
        output, _ = self.proc.communicate()
        # Process has terminated by now, so we can iterate without keeping it alive.
        # for line in splitlines_iter(str(output, 'utf-8')):
        for line in splitlines_iter(output):
            yield line


class StreamedProcessReaderContext(ProcessReaderContext):
    def readlines(self):
        """Usage:

            cmd = 'ls -l'
            reader = StreamedProcessReaderContext(cmd)
            with reader:
                for line in reader.readlines():
                    print line

        Any exception within the 'with-block' is propagated.
        Otherwise, after all lines are read, if 'cmd' failed, Exception is raised.
        """
        for line in self.proc.stdout:
            # We expect unicode from py3 but raw-str from py2, given
            # universal_newlines=True.
            # Based on source-code in 'future/types/newstr.py',
            # it seems that str(str(x)) has no extra penalty,
            # and it should not crash either. Anyway,
            # our tests would catch it.
            # yield str(line, 'utf-8').rstrip()
            yield line.rstrip()


def filesize(fn):
    """In bytes.
    Raise if fn does not exist.
    """
    statinfo = os.stat(fn)
    return statinfo.st_size


def validated_fns(fofn):
    return list(yield_validated_fns(fofn))


def yield_validated_fns(fofn):
    """Return list of filenames from fofn, either abs or relative to CWD instead of dir of fofn.
    Assert none are empty/non-existent.
    """
    dirname = os.path.normpath(os.path.dirname(fofn))  # normpath makes '' become '.'
    try:
        fns = deserialize(fofn)
    except:
        # LOG('las fofn {!r} does not seem to be JSON or msgpack; try to switch, so we can detect truncated files.'.format(fofn))
        fns = open(fofn).read().strip().split()
    try:
        for fn in fns:
            assert fn
            if not os.path.isabs(fn):
                fn = os.path.normpath(os.path.join(dirname, fn))
            assert os.path.isfile(fn), 'File {!r} is not a file.'.format(fn)
            assert filesize(fn), '{!r} has size {}'.format(fn, filesize(fn))
            yield fn
    except Exception:
        sys.stderr.write('Failed to validate FOFN {!r}\n'.format(fofn))
        raise


@contextlib.contextmanager
def TemporaryDirectory():
    name = tempfile.mkdtemp()
    LOG('TemporaryDirectory={!r}'.format(name))
    try:
        yield name
    finally:
        shutil.rmtree(name)


if sys.version_info >= (3, 0):
    NativeIO = io.StringIO
else:
    NativeIO = io.BytesIO

LOG = logging.getLogger()


def log(*msgs):
    LOG.debug(' '.join(repr(m) for m in msgs))


def eng(number):
    return '{:.1f}MB'.format(number / 2 ** 20)


class Percenter(object):
    """Report progress by golden exponential.

    Usage:
        counter = Percenter('mystruct', total_len(mystruct))

        for rec in mystruct:
            counter(len(rec))
    """

    def __init__(self, name, total, log=LOG.info, units='units'):
        if sys.maxint == total:
            log('Counting {} from "{}"'.format(units, name))
        else:
            log('Counting {:,d} {} from\n  "{}"'.format(total, units, name))
        self.total = total
        self.log = log
        self.name = name
        self.units = units
        self.call = 0
        self.count = 0
        self.next_count = 0
        self.a = 1  # double each time

    def __call__(self, more, label=''):
        self.call += 1
        self.count += more
        if self.next_count <= self.count:
            self.a = 2 * self.a
            self.a = max(self.a, more)
            self.a = min(self.a, (self.total - self.count), round(self.total / 10.0))
            self.next_count = self.count + self.a
            if self.total == sys.maxint:
                msg = '{:>10} count={:15,d} {}'.format(
                    '#{:,d}'.format(self.call), self.count, label)
            else:
                msg = '{:>10} count={:15,d} {:6.02f}% {}'.format(
                    '#{:,d}'.format(self.call), self.count, 100.0 * self.count / self.total, label)
            self.log(msg)

    def finish(self):
        self.log('Counted {:,d} {} in {} calls from:\n  "{}"'.format(
            self.count, self.units, self.call, self.name))



def FilePercenter(fn, log=LOG.info):
    if '-' == fn or not fn:
        size = sys.maxint
    else:
        size = filesize(fn)
    return Percenter(fn, size, log, units='bytes')

# @contextlib.contextmanager
# def open_progress(fn, mode='r', log=LOG.info):
#     """
#     Usage:
#         with open_progress('foo', log=LOG.info) as stream:
#             for line in stream:
#                 use(line)
#
#     That will log progress lines.
#     """
#     def get_iter(stream, progress):
#         for line in stream:
#             progress(len(line))
#             yield line
#
#     fp = FilePercenter(fn, log=log)
#     with open(fn, mode=mode) as stream:
#         yield get_iter(stream, fp)
#     fp.finish()

# def read_as_msgpack(stream):
#     import msgpack
#     content = stream.read()
#     log('  Read {} as msgpack'.format(eng(len(content))))
#     return msgpack.loads(content)


def read_as_json(stream):
    import json
    content = stream.read()
    log('  Read {} as json'.format(eng(len(content))))
    return json.loads(content)


# def write_as_msgpack(stream, val):
#     import msgpack
#     content = msgpack.dumps(val)
#     log('  Serialized to {} as msgpack'.format(eng(len(content))))
#     stream.write(content)


def write_as_json(stream, val):
    import json
    content = json.dumps(val, indent=2, separators=(',', ': '))
    log('  Serialized to {} as json'.format(eng(len(content))))
    stream.write(content)


def deserialize(fn):
    log('Deserializing from {!r}'.format(fn))
    with open(fn) as ifs:
        log('  Opened for read: {!r}'.format(fn))

        if fn.endswith('.json'):
            val = read_as_json(ifs)
        else:
            raise Exception('Unknown extension for {!r}'.format(fn))
    log('  Deserialized {} records'.format(len(val)))
    return val


def serialize(fn, val):
    """Assume dirname exists.
    """
    log('Serializing {} records'.format(len(val)))
    mkdirs(os.path.dirname(fn))
    with open(fn, 'w') as ofs:
        log('  Opened for write: {!r}'.format(fn))

        if fn.endswith('.json'):
            write_as_json(ofs, val)
            ofs.write('\n')  # for vim
        else:
            raise Exception('Unknown extension for {!r}'.format(fn))


def yield_abspath_from_fofn(fofn_fn):
    """Yield each filename.
    Relative paths are resolved from the FOFN directory.
    'fofn_fn' can be .fofn, .json, .msgpack
    """
    try:
        fns = deserialize(fofn_fn)
    except:
        # LOG('las fofn {!r} does not seem to be JSON; try to switch, so we can detect truncated files.'.format(fofn_fn))
        fns = open(fofn_fn).read().strip().split()
    try:
        basedir = os.path.dirname(fofn_fn)
        for fn in fns:
            if not os.path.isabs(fn):
                fn = os.path.abspath(os.path.join(basedir, fn))
            yield fn
    except Exception:
        LOG.error('Problem resolving paths in FOFN {!r}'.format(fofn_fn))
        raise


def rmdirs(*dirnames):
    for d in dirnames:
        assert os.path.normpath(d.strip()) not in ['.', '', '/']
    os.syscall('rm -rf {}'.format(' '.join(dirnames)))


def rmdir(d):
    rmdirs(d)


def rm_force(*fns):
    for fn in fns:
        if os.path.exists(fn):
            os.unlink(fn)
