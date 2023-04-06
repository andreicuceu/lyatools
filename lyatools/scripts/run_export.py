#!/usr/bin/env python

import argparse
from lyatools.multi_run import multi_export


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input-dir', type=str, default=None, required=True,
                        help='Directory that contains all of the picca outputs')

    parser.add_argument('--qq-seeds', type=str, required=True, nargs='*',
                        help='Which seeds to run qq on. Either integers or range e.g. 0-5')

    parser.add_argument('--run-type', type=str, required=True,
                        help='Directory names of the picca runs')

    parser.add_argument('--analysis-name', type=str, required=True,
                        help='Analysis name. One run can have multiple analyses')

    parser.add_argument('--add-dmat', action='store_true', required=False,
                        help='Flag for adding distortion matrices')

    parser.add_argument('--dmat-path', type=str, default=None, required=False,
                        help='Path to correlations dir with distortion matrices')

    parser.add_argument('--name-string', type=str, default=None, required=False,
                        help='Optional name string to append to the end of the filenames')

    parser.add_argument('--stack-correlations', action="store_true", default=False, required=False,
                        help='Stack the correlations in multiple realizations')

    parser.add_argument('--stack-out-dir', type=str, default=None, required=False,
                        help='Path for stack output')

    args = parser.parse_args()

    multi_export(args)


if __name__ == '__main__':
    main()
