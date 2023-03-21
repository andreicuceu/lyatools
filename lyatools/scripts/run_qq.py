#!/usr/bin/env python

import argparse
from lyatools.quickquasars import multi_run_qq, QQ_RUN_ARGS


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-i', '--input-dir', type=str, required=False,
                        default='/global/cfs/projectdirs/desi/mocks/lya_forest/london/v9.0/',
                        help='Raw directory with all v9.0 lyacolore runs')

    parser.add_argument('-o', '--output-dir', type=str, required=False,
                        default=('/global/cfs/projectdirs/desi/mocks/lya_forest/develop/'
                                 'london/qq_desi/v9.0'),
                        help='Directory that contains all of the qq realisations (dir with v9.0.x)')

    parser.add_argument('--qq-seeds', type=str, required=True, nargs='*',
                        help='Which seeds to run qq on. Either integers or range e.g. 0-5')

    parser.add_argument('--qq-run-type', type=str, required=True, choices=list(QQ_RUN_ARGS.keys()),
                        help='Directory names of the qq runs')

    parser.add_argument('--nersc-machine', type=str, default='perl', choices=['perl', 'cori'],
                        required=False, help='Whether script is to be run on Perlmutter or Cori')

    parser.add_argument('--slurm-hours', type=float, default=0.5, required=False,
                        help='Number of hours for slurm job')

    parser.add_argument('--slurm-queue', type=str, default='regular', required=False,
                        help='Slurm queue to use')

    parser.add_argument('--nodes', type=int, default=8, required=False,
                        help='Number of nodes')

    parser.add_argument('--nproc', type=int, default=32, required=False,
                        help='Number of processors')

    parser.add_argument('--env-command', type=str, default=None, required=False,
                        help='Command that activates your environment. Must be a bash function')

    parser.add_argument('--no-submit', action="store_true", default=False, required=False,
                        help='make the run scripts but do not submit the jobs')

    parser.add_argument('--test-run', action="store_true", default=False, required=False,
                        help='If enabled, uses only first 10 transmission files and\
                        sends all jobs to the debug queue with a basic setup.')

    args = parser.parse_args()

    multi_run_qq(args.input_dir, args.output_dit, args.qq_seeds, args.qq_run_type, args.test_run,
                 args.no_submit, args.nersc_machine, args.slurm_hours, args.slurm_queue,
                 args.nodes, args.nproc, args.env_command)


if __name__ == '__main__':
    main()
