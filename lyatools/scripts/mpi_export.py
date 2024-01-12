#!/usr/bin/env python3
import sys
import argparse
from mpi4py import MPI
from subprocess import run

from lyatools import submit_utils


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--commands", type=str, required=True, nargs='*',
                        help="The picca export commands to run.")
    parser.add_argument("-l", "--log-path", type=str, required=True,
                        help="The path to the log files.")

    args = parser.parse_args()

    def print_func(message):
        print(f'Rank {cpu_rank}: {message}')
        sys.stdout.flush()

    mpi_comm = MPI.COMM_WORLD
    cpu_rank = mpi_comm.Get_rank()
    num_cpus = mpi_comm.Get_size()
    if num_cpus > len(args.commands):
        raise ValueError(f'There are more cores ({num_cpus}) than commands ({len(args.commands)}).')

    num_tasks_per_proc = len(args.commands) // num_cpus
    remainder = len(args.commands) % num_cpus
    if cpu_rank < remainder:
        start = int(cpu_rank * (num_tasks_per_proc + 1))
        stop = int(start + num_tasks_per_proc + 1)
    else:
        start = int(cpu_rank * num_tasks_per_proc + remainder)
        stop = int(start + num_tasks_per_proc)

    print_func(f'Running export indexes {start} to {stop}.')

    for i in range(start, stop):
        print_func(f'Running command: {args.commands[i]}')
        process = run(args.commands[i], shell=True, capture_output=True)
        if process.returncode != 0:
            raise ValueError(f'Running {args.commands[i]} returned non-zero exitcode '
                             f'with error {process.stderr}')

        with open(f'{args.log_path}/export_{i}.log', 'w') as f:
            f.write(process.stdout.decode('utf-8'))
        print_func(f'Finished export index {i}.')


if __name__ == '__main__':
    main()
