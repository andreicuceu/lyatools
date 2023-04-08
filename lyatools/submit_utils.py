import os
import numpy as np
from subprocess import run
from pathlib import Path
from typing import Union

import lyatools


def get_seed_list(qq_seeds):
    # Get list of seeds
    seed_list = qq_seeds.split(',')

    run_seeds = []
    for seed in seed_list:
        seed_range = seed.split('-')

        if len(seed_range) == 1:
            run_seeds.append(int(seed_range[0]))
        elif len(seed_range) == 2:
            run_seeds += list(np.arange(int(seed_range[0]), int(seed_range[1])))
        else:
            raise ValueError(f'Unknown seed type {seed}. Must be int or range (e.g. 0-5)')

    run_seeds.sort()
    return run_seeds


def make_header(machine: str = 'perl', queue: str = 'regular', nodes: int = int(1),
                omp_threads: int = int(128), time: Union[str, float] = '01:00:00',
                job_name: str = 'run_script', err_file: Union[str, Path] = 'run-%j.err',
                out_file: Union[str, Path] = 'run-%j.out'):
    """
    Makes a string header for submitting slurm jobs at NERSC. Supports both Perlmutter and Cori.
    Args:
        machine: string - default: 'perl'
            Which machine to use, choose from ['perl', 'cori'].
        queue: string - default: 'regular'
            Which queue to submit to.
        nodes: int - default: 1
            How many nodes to request.
        time: string - default: '01:00:00'
            What amount of time to request, in "hh:mm:ss" format. If given as
            a float, a warning will be raised and it will be converted to the
            desired format.
        job_name: string - default: 'run_script'
            What name to give the job.
        err_file: string - default: 'run-%j.err'
            Name of file to write errors to.
        out_file: string - default: 'run-%j.out'
            Name of file to write output to.
    Returns:
        header: string
            Contents of header as a string.
    """
    if isinstance(time, float):
        time = convert_job_time(time)
    assert isinstance(time, str), "make_header called with time variable of unknown type"

    omp_threads = int(omp_threads)
    if machine == 'perl':
        machine_string = 'cpu'
        assert omp_threads <= 128
    elif machine == 'cori':
        machine_string = 'haswell'
        assert omp_threads <= 64
    else:
        raise ValueError(f'make_header called with unknown machine name {machine}.'
                         ' Choose from ["perl", "cori"].')

    header = ''
    header += '#!/bin/bash -l\n\n'

    header += f'#SBATCH --qos {queue}\n'
    header += f'#SBATCH --nodes {nodes}\n'
    header += f'#SBATCH --time {time}\n'
    header += f'#SBATCH --job-name {job_name}\n'
    header += f'#SBATCH --error {err_file}\n'
    header += f'#SBATCH --output {out_file}\n'
    header += f'#SBATCH -C {machine_string}\n'
    header += '#SBATCH -A desi\n\n'

    header += 'umask 0027\n'
    header += f'export OMP_NUM_THREADS={omp_threads}\n\n'

    return header


def write_script(script_path, text):
    with open(script_path, 'w+') as f:
        f.write(text)

    make_file_executable(script_path)


def run_job(script, dependency_ids=None, no_submit=False):
    """Make a job script and run it

    Parameters
    ----------
    script : str
        Path where script will pe written
    no_submit : bool, optional
        flag for submitting the job, by default False
    """
    dependency = ""
    if isinstance(dependency_ids, int) and dependency_ids > 0:
        dependency = f"--dependency=afterok:{dependency_ids} "
    elif isinstance(dependency_ids, list) and len(dependency_ids) > 0:
        valid_deps = [str(j) for j in dependency_ids if (j is not None and j > 0)]
        if valid_deps:
            dependency = f"--dependency=afterok:{':'.join(valid_deps)} "

    command = f"sbatch {dependency}{script} | tr -dc '0-9'"

    jobid = None
    if not no_submit:
        print(f'Submitting script {script}')
        process = run(command, capture_output=True)

        if process.returncode != 0:
            raise ValueError(f'Running "sbatch {dependency}{script}" returned non-zero exitcode '
                             f'with error {process.stderr}')

        jobid = int(process.stdout)
    else:
        print(f'No submit active. Command prepared: {command}')

    return jobid


def convert_job_time(num_hours: float) -> str:
    """Converts a float number of hours into a string of "hh:mm:ss"

    Parameters
    ----------
    num_hours : float
        Number of hours to convert.

    Returns
    -------
    str
        Time string
    """
    hours = int(np.floor(num_hours))

    num_minutes_rest = (num_hours - hours) * 60
    minutes = int(np.floor(num_minutes_rest))

    num_seconds_rest = (num_minutes_rest - minutes) * 60
    seconds = int(np.ceil(num_seconds_rest))

    if seconds == 60:
        seconds = 0
        minutes += 1

    return '{:02d}:{:02d}:{:02d}'.format(hours, minutes, seconds)


def print_spacer_line() -> None:
    """
    Prints an 80 character line of "#"s, with a blank line before and after.
    """
    print('')
    print('#'*80)
    print('')


def set_umask(mask: str = '0027') -> None:
    """Sets the value of umask.

    Parameters
    ----------
    mask : str, optional
        mask value, by default '0027'
    """
    _ = os.umask(int(mask, 8))


def make_file_executable(f: Path) -> None:
    """Make a file executable

    Parameters
    ----------
    f : str
        file
    """
    run(['chmod', 'ug+x', f])


def find_path(path, enforce=True):
    """ Find paths on the system.
    Parameters
    ----------
    path : string
        Input path. Can be absolute or relative to lyatools
    enforce : bool
        Flag for enforcing that the path exists
    """
    input_path = Path(os.path.expandvars(path))

    # First check if it's an absolute path
    if input_path.exists():
        return input_path.resolve()

    # Get the lyatools path and check inside lyatools (this returns lyatools/lyatools)
    lyatools_path = Path(os.path.dirname(lyatools.__file__))

    # Check the lyatools folder
    in_lyatools = lyatools_path / input_path
    if in_lyatools.exists():
        return in_lyatools.resolve()

    # Check if it's something used for tests
    in_tests = lyatools_path.parents[0] / 'tests' / input_path
    if in_tests.exists():
        return in_tests.resolve()

    # Check from the main lyatools folder
    in_main = lyatools_path.parents[0] / input_path
    if in_main.exists():
        return in_main.resolve()

    if not enforce:
        print(f'Warning, the path/file was not found: {input_path}')
        return input_path
    else:
        raise RuntimeError(f'The path/file does not exist: {input_path}')
