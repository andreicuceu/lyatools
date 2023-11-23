#!/usr/bin/env python3
import sys
import argparse
import numpy as np
import configparser
from mpi4py import MPI
from pathlib import Path

from lyatools import submit_utils
from lyatools.vegafit import run_vega_fitter


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--config-file", type=str, required=True,
                        help="The path to the lyatools-vega configuration file.")

    args = parser.parse_args()

    mpi_comm = MPI.COMM_WORLD
    cpu_rank = mpi_comm.Get_rank()
    num_cpus = mpi_comm.Get_size()

    def print_func(message):
        if cpu_rank == 0:
            print(message)
        sys.stdout.flush()
        mpi_comm.barrier()

    config = configparser.ConfigParser()
    config.read(args.config_file)

    analysis_dir = Path(config['mock_setup'].get('analysis_dir'))
    mock_version = config['mock_setup'].get('mock_version')
    qq_seeds = config['mock_setup'].get('qq_seeds')
    qq_run_type = config['mock_setup'].get('qq_run_type')
    analysis_name = config['mock_setup'].get('analysis_name', 'baseline')
    output_dir = config['mock_setup'].get('output_dir')
    run_flag = config['mock_setup'].getboolean('run_flag', True)

    seeds = np.array(submit_utils.get_seed_list(qq_seeds))

    num_tasks_per_proc = len(seeds) // num_cpus
    remainder = len(seeds) % num_cpus
    if cpu_rank < remainder:
        start = int(cpu_rank * (num_tasks_per_proc + 1))
        stop = int(start + num_tasks_per_proc + 1)
    else:
        start = int(cpu_rank * num_tasks_per_proc + remainder)
        stop = int(start + num_tasks_per_proc)

    print_func(f'Rank {cpu_rank} running seeds {start} to {stop}.')

    for seed in seeds[start:stop]:
        version = f'{mock_version}.{seed}'
        name_extension = config['fit_info'].get('name_extension', None)
        if name_extension is not None:
            config['fit_info']['name_extension'] = f'{name_extension}_{version}'
        else:
            config['fit_info']['name_extension'] = f'{version}'

        corr_path = analysis_dir / f'{version}' / f'{qq_run_type}'
        corr_path = corr_path / f'{analysis_name}' / 'correlations'

        run_vega_fitter(config, corr_path, output_dir, run_flag=run_flag)


if __name__ == '__main__':
    main()
