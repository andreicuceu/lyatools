#!/usr/bin/env python3

import argparse

from lyatools import submit_utils
from lyatools.dla_catalog import make_dla_catalog


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input-dir", type=str, required=True,
                        help="the spectra-16 directory containing truth files")

    parser.add_argument("-o", "--output-dir", type=str, required=True,
                        help="Name of output directory")

    parser.add_argument('--mask-nhi-cut', type=float, default=None, required=False,
                        help=('NHI cut used to select DLAs for masking. '
                              '(Only DLAs with NHI larger than this will be masked)'))

    parser.add_argument("--nproc", type=int, default=None, required=False,
                        help='Number cores for parallelization')

    args = parser.parse_args()

    make_dla_catalog(args.input_dir, args.output_dir, args.mask_nhi_cut, nproc=args.nproc)


if __name__ == '__main__':
    main()
