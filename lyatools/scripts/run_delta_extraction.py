#!/usr/bin/env python

import argparse
from lyatools.multi_run import multi_run_delta_extraction


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input-dir', type=str, default=None, required=True,
                        help='Directory containing spectra files')

    parser.add_argument('-o', '--output-dir', type=str, default=None, required=True,
                        help='Path to output directory')

    parser.add_argument('--qq-seeds', type=str, required=True, nargs='*',
                        help='Which seeds to run qq on. Either integers or range e.g. 0-5')

    parser.add_argument('--qq-run-type', type=str, required=True,
                        help='Directory names of the qq runs')

    parser.add_argument('--delta-lambda', type=float, default=2.4, required=False,
                        help=('Bin size for wavelength grid [Angstrom]'))

    parser.add_argument('--raw-dir', type=str, default=None, required=False,
                        help='Raw directory with all v9.0 lyacolore runs')

    parser.add_argument('--raw-name', type=str, default='desi-3-raw', required=False,
                        help='Name for raw directory in the picca_on_mocks folders')

    parser.add_argument('--run-true-continuum', action="store_true",
                        help='Whether to run the true continuum analyses.')

    parser.add_argument('--raw-stats-file', type=str, default=None, required=False,
                        help='Path to stats file written by raw analysis.')

    parser.add_argument('--run-lya-region', action="store_true",
                        help='Whether to run the lya region.')

    parser.add_argument('--run-lyb-region', action="store_true",
                        help='Whether to run the lyb region.')

    parser.add_argument('--no-run-continuum-fitting', action="store_true",
                        help='Flag to turn off continuum fitting (main delta extraction analysis).')

    parser.add_argument('--no-force-stack-delta-to-zero', action="store_true",
                        help='Whether to deactivate the force stack delta to zero option')

    parser.add_argument('--num-pix-min', type=int, default=150, required=False,
                        help='Minimum number of rebined pixels to accept forest.')

    parser.add_argument('--max-num-spec', type=int, default=None, required=False,
                        help='Maximum number of spectra to process')

    parser.add_argument('--nersc-machine', type=str, default='perl', choices=['perl', 'cori'],
                        required=False, help='Whether script is to be run on Perlmutter or Cori')

    parser.add_argument('--slurm-hours', type=float, default=0.5, required=False,
                        help='Number of hours for slurm job')

    parser.add_argument('--slurm-queue', type=str, default='regular', required=False,
                        help='Slurm queue to use')

    parser.add_argument('--nproc', type=int, default=64, required=False,
                        help='Number of processors')

    parser.add_argument('--env-command', type=str, default='piccaenv', required=False,
                        help='Command to activate the anaconda environment that contains picca.')

    parser.add_argument('--no-submit', action="store_true", default=False, required=False,
                        help='make the run scripts but do not submit the jobs')

    parser.add_argument('--test-run', action="store_true", default=False, required=False,
                        help='If enabled, uses only first 10 files and\
                        sends all jobs to the debug queue with a basic setup.')

    args = parser.parse_args()

    multi_run_delta_extraction(args)


if __name__ == '__main__':
    main()
