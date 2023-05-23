#!/usr/bin/env python3

import argparse

from lyatools import submit_utils
from lyatools.stack import stack_export_correlations


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("--data", type=str, nargs="*", required=True,
                        help="the (x)cf....fits files to be coadded")

    parser.add_argument("--out", type=str, required=True, help="name of output file")

    parser.add_argument('--dmat', type=str, default=None, required=False,
                        help=('Distortion matrix produced via picca_dmat.py, picca_xdmat.py... '
                              '(if not provided will be identity)'))

    parser.add_argument("--shuffled-correlations", type=str, nargs="*", required=True,
                        help="the xcf.... shuffled correlation files to be subtracted")

    args = parser.parse_args()

    stack_export_correlations(args.data, args.out, args.dmat)


if __name__ == '__main__':
    main()
