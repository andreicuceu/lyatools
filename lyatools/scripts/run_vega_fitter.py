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
        print(f'Rank {cpu_rank}: {message}')
        sys.stdout.flush()

    config = configparser.ConfigParser()
    config.optionxform = lambda option: option
    config.read(args.config_file)

    analysis_dir = Path(config['mock_setup'].get('analysis_dir'))
    mock_version = config['mock_setup'].get('mock_version')
    qq_run_type = config['mock_setup'].get('qq_run_type')
    analysis_name = config['mock_setup'].get('analysis_name', 'baseline')
    output_dir = config['mock_setup'].get('output_dir')
    run_flag = config['mock_setup'].getboolean('run_flag', True)
    name_extension = config['fit_info'].get('name_extension', None)
    cat_path = config['mock_setup'].get('cat_path', None)

    input_seeds = config['mock_setup'].get('input_seeds', None)
    cat_seeds = config['mock_setup'].get('cat_seeds', None)
    qq_seeds = config['mock_setup'].get('qq_seeds')
    inverted_cat_seed = config['mock_setup'].getboolean('inverted_cat_seed', False)

    if input_seeds is None:
        input_seeds = qq_seeds
    if cat_seeds is None:
        cat_seeds = qq_seeds

    seeds = np.array(submit_utils.get_seed_list(qq_seeds))
    input_seeds = submit_utils.get_seed_list(input_seeds)
    cat_seeds = submit_utils.get_seed_list(cat_seeds)

    if len(input_seeds) != len(seeds):
        raise ValueError('Number of input seeds and qq seeds must match.')
    if len(cat_seeds) != len(seeds):
        raise ValueError('Number of catalog seeds and qq seeds must match.')

    num_tasks_per_proc = len(seeds) // num_cpus
    remainder = len(seeds) % num_cpus
    if cpu_rank < remainder:
        start = int(cpu_rank * (num_tasks_per_proc + 1))
        stop = int(start + num_tasks_per_proc + 1)
    else:
        start = int(cpu_rank * num_tasks_per_proc + remainder)
        stop = int(start + num_tasks_per_proc)

    print_func(f'Running seeds {start} to {stop}.')

    for i in range(start, stop):
        if inverted_cat_seed:
            version = f'{mock_version}.{input_seeds[i]}.{cat_seeds[i]}i.{seeds[i]}'
        else:
            version = f'{mock_version}.{input_seeds[i]}.{cat_seeds[i]}.{seeds[i]}'

        if name_extension is not None:
            config['fit_info']['name_extension'] = f'{name_extension}_{version}'
        else:
            config['fit_info']['name_extension'] = f'{version}'

        corr_path = analysis_dir / version / qq_run_type
        corr_path = corr_path / analysis_name / 'correlations'

        qq_cat_path = None
        if cat_path is not None:
            qq_cat_path = Path(cat_path) / version / qq_run_type / 'zcat_gauss_400.fits'

        run_vega_fitter(config, corr_path, output_dir, qq_cat_path, run_flag=run_flag)
        print_func(f'Finished seed {seeds[i]}.')


if __name__ == '__main__':
    main()
