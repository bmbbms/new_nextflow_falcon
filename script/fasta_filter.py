#! /usr/bin/env python2.7

import FastaReader as FastaReader

import os
import sys
import argparse
import collections
import itertools
import logging
import contextlib

LOG = logging.getLogger()

ZMWTuple = collections.namedtuple('ZMWTuple', ['movie_name', 'zmw_id', 'subread_start', 'subread_end', 'seq_len', 'subread_record', 'subread_header', 'subread_id'])

def tokenize_header(seq_header):
    rid = seq_header.split()[0]
    movie_name, zmw_id, subread_pos = rid.split('/')
    subread_start, subread_end = [int(val) for val in subread_pos.split('_')]
    return movie_name, zmw_id, subread_start, subread_end

def median_zmw_subread(zmw_subreads):
    """Return subread_record with median seq_len.
    zmw_subreads is a list of ZMWTuple.
    """
    assert len(zmw_subreads) != 0

    sorted_subreads = sorted(zmw_subreads, key = lambda x: x.seq_len)

    # Not really a median since we round to the floor, and the definition of
    # median value would be the average of the two middle elements.
    # However, we need a concrete subread associated with the median value.
    median_id = len(sorted_subreads) // 2

    return sorted_subreads[median_id]

def internal_median_zmw_subread(zmw_subreads):
    """Returns a single subread based on the following selection criteria:
    - If the ZMW has < 3 subreads, the maximum one is output.
    - If the ZMW has >= 3 subreads, the median one is selected only from the internal
      ones (ignoring the first and the last subread)
    This is intended to prevent the impact of very short first and last subread on the
    median selection, since the polymerase can start/stop in the middle of the insert.
    zmw_subreads is a list of ZMWTuple.
    """
    assert len(zmw_subreads) != 0

    selected_subread = None

    if len(zmw_subreads) < 3:
        sorted_subreads = sorted(zmw_subreads, key = lambda x: x.seq_len)
        selected_subread = sorted_subreads[-1]
    else:
        sorted_by_pos = sorted(zmw_subreads, key = lambda x: x.subread_start)
        sorted_subreads = sorted(sorted_by_pos[1:-1], key = lambda x: x.seq_len)
        median_id = len(sorted_subreads) // 2
        selected_subread = sorted_subreads[median_id]

    return selected_subread

##############################
### Streamed-median filter ###
##############################
def yield_zmwtuples(records):
    for record in records:
        movie_name, zmw_id, subread_start, subread_end = tokenize_header(record.name)
        zrec = ZMWTuple(movie_name=movie_name, zmw_id=zmw_id,
                        subread_start=subread_start, subread_end=subread_end,
                        seq_len=len(record.sequence), subread_record=record,
                        subread_header=record.name, subread_id=0)
        yield zrec

def run_streamed_median(fp_in, fp_out, fn='-', zmw_filter_func=median_zmw_subread):
    fasta_records = FastaReader.yield_fasta_records(fp_in, fn, log=LOG.info)
    for zmw_id, zmw_subreads in itertools.groupby(yield_zmwtuples(fasta_records), lambda x: x.zmw_id):
        median_zrec = median_zmw_subread(list(zmw_subreads))
        fp_out.write(str(median_zrec.subread_record))
        fp_out.write('\n')

##############################
### Pass filter.           ###
##############################
def run_pass_filter(fp_in, fp_out, fn):
    for record in FastaReader.yield_fasta_records(fp_in, fn, log=LOG.info):
        fp_out.write(str(record))
        fp_out.write('\n')

##################################
### Double-pass median filter. ###
##################################
def run_median_filter(fp_in, fp_out, fn, zmw_filter_func=median_zmw_subread):
    # Expect an actual file, not a stream.
    assert(os.path.exists(fn))

    # Needed to jump back for the second pass.
    fp_in_start = fp_in.tell()

    # Stores all subreads for a ZMW.
    zmw_dict = collections.defaultdict(list)

    # First pass, collect all ZMW info.
    for record in FastaReader.yield_fasta_records(fp_in, fn, log=LOG.info):
        movie_name, zmw_id, subread_start, subread_end = tokenize_header(record.name)
        # Store None instead of the actual record to free the memory after yield.
        zrec = ZMWTuple(movie_name=movie_name, zmw_id=zmw_id,
                        subread_start=subread_start, subread_end=subread_end,
                        seq_len=len(record.sequence), subread_record=None,
                        subread_header=record.name, subread_id=len(zmw_dict[zmw_id]))
        zmw_dict[zmw_id].append(zrec)

    # For each ZMW, keep only one particular subread, specified by it's order of
    # appearance in the input FASTA file (stored in median_zrec.subread_id).
    whitelist = collections.defaultdict(int)
    for zmw_id, zmw_subreads in zmw_dict.iteritems():
        median_zrec = zmw_filter_func(list(zmw_subreads))
        whitelist[zmw_id] = median_zrec.subread_id

    # Second pass, yield selected sequences.
    # Rewind.
    fp_in.seek(fp_in_start, os.SEEK_SET)
    # Fly-through.
    for record in FastaReader.yield_fasta_records(fp_in, fn, log=LOG.info):
        movie_name, zmw_id, subread_start, subread_end = tokenize_header(record.name)
        # Write-out only one particular subread from the ZMW.
        if whitelist[zmw_id] == 0:
            fp_out.write(str(record))
            fp_out.write('\n')
        whitelist[zmw_id] -= 1

###############################
### Internal median filter. ###
###############################
def run_internal_median_filter(fp_in, fp_out, fn):
    run_median_filter(fp_in, fp_out, fn, zmw_filter_func=internal_median_zmw_subread)

#######################################
### Streamed internal median filter ###
#######################################
def run_streamed_internal_median_filter(fp_in, fp_out, fn='-'):
    run_streamed_median(fp_in, fp_out, fn=fn, zmw_filter_func=internal_median_zmw_subread)

##############################
### Main and cmds.         ###
##############################
@contextlib.contextmanager
def open_stream(input_path):
    if input_path == '-':
        yield sys.stdin
    else:
        with open(input_path) as stream:
            yield stream

def cmd_run_pass_filter(args):
    with open_stream(args.input_path) as fp_in:
        run_pass_filter(fp_in, sys.stdout, args.input_path)

def cmd_run_streamed_median_filter(args):
    with open_stream(args.input_path) as fp_in:
        run_streamed_median(fp_in, sys.stdout, args.input_path)

def cmd_run_median_filter(args):
    # Don't allow '-' for the double-pass median filter.
    with open(args.input_path, 'r') as fp_in:
        run_median_filter(fp_in, sys.stdout, args.input_path)

def cmd_run_internal_median_filter(args):
    with open(args.input_path, 'r') as fp_in:
        run_internal_median_filter(fp_in, sys.stdout, args.input_path)

def cmd_run_streamed_internal_median_filter(args):
    with open_stream(args.input_path) as fp_in:
        run_streamed_internal_median_filter(fp_in, sys.stdout, args.input_path)

class HelpF(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass

def parse_args(argv):
    description = 'Filters the input FASTA file according to one of the selected filters.'
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=HelpF,
    )

    subparsers = parser.add_subparsers(help='sub-command help')

    help_pass = 'The no-op filter - passes every FASTA record to stdout. If input_path is "-", input is read from stdin.'
    help_median = 'Applies the median-length ZMW filter by running two passes over the data. Only one subread per ZMW is output, based on median-length selection. The input_path needs to be a file.'
    help_streamed_median = 'Applies the median-length ZMW filter by running a single-pass over the data. The input subreads should be groupped by ZMW. If input_path is "-", input is read from stdin.'
    help_internal_median = 'Applies the median-length ZMW filter only on internal subreads (ZMWs with >= 3 subreads) by running two passes over the data. For ZMWs with < 3 subreads, the maximum-length one is selected. The input_path needs to be a file.'
    help_streamed_internal_median = 'Applies the median-length ZMW filter only on internal subreads (ZMWs with >= 3 subreads) by running a single pass over the data. The input subreads should be groupped by ZMW. For ZMWs with < 3 subreads, the maximum-length one is selected. If input_path is "-", input is read from stdin.'

    parser_pass = subparsers.add_parser('pass',
            formatter_class=HelpF,
            description=help_pass,
            help=help_pass)
    parser_pass.set_defaults(func=cmd_run_pass_filter)

    parser_median = subparsers.add_parser('median',
            formatter_class=HelpF,
            description=help_median,
            help=help_median)
    parser_median.set_defaults(func=cmd_run_median_filter)

    parser_streamed_median = subparsers.add_parser('streamed-median',
            formatter_class=HelpF,
            description=help_streamed_median,
            help=help_streamed_median)
    parser_streamed_median.set_defaults(func=cmd_run_streamed_median_filter)

    parser_internal_median = subparsers.add_parser('internal-median',
            formatter_class=HelpF,
            description=help_internal_median,
            help=help_internal_median)
    parser_internal_median.set_defaults(func=cmd_run_internal_median_filter)

    parser_streamed_internal_median = subparsers.add_parser('streamed-internal-median',
            formatter_class=HelpF,
            description=help_streamed_internal_median,
            help=help_streamed_internal_median)
    parser_streamed_internal_median.set_defaults(func=cmd_run_streamed_internal_median_filter)

    parser.add_argument('input_path', help='Input PacBio FASTA file')

    args = parser.parse_args(argv[1:])
    return args

def main(argv=sys.argv):
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    args.func(args)

if __name__ == "__main__":  # pragma: no cover
    main(sys.argv)          # pragma: no cover
