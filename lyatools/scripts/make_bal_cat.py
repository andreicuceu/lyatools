#!/usr/bin/env python3

import argparse

from lyatools import submit_utils
from lyatools.bal_catalog import make_bal_catalog


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input-dir", type=str, required=True,
                        help="the spectra-16 directory containing truth files")

    parser.add_argument("-o", "--output-dir", type=str, required=True,
                        help="Name of output directory")

    parser.add_argument("--ai-cut", type=int, default=500, required=False,
                        help='Cut threshold in AI')

    parser.add_argument("--bi-cut", type=int, default=None, required=False,
                        help='Cut threshold in BI')

    parser.add_argument("--nproc", type=int, default=None, required=False,
                        help='Number cores for parallelization')

    args = parser.parse_args()

    make_bal_catalog(
        args.input_dir, args.output_dir, ai_cut=args.ai_cut, bi_cut=args.bi_cut, nproc=args.nproc
    )


if __name__ == '__main__':
    main()
