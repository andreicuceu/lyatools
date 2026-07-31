import os
import numpy as np
from subprocess import run
from pathlib import Path
from typing import Union

import lyatools


def get_seed_list(qq_seeds):
    """Parse a seed specification string into a sorted list of integer seeds.

    Parameters
    ----------
    qq_seeds : str
        Comma-separated integers or ranges, e.g. '0,2,5-8'.

    Returns
    -------
    list of int
        Sorted list of seed integers.
    """
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
    """Generate a SLURM batch script header string for NERSC Perlmutter or Cori.

    Parameters
    ----------
    machine : str, optional
        Target machine; one of 'perl' (Perlmutter) or 'cori'.
    queue : str, optional
        SLURM QOS/queue name (e.g. 'regular', 'debug').
    nodes : int, optional
        Number of compute nodes to request.
    omp_threads : int, optional
        OMP_NUM_THREADS value; max 256 for Perlmutter, 64 for Cori.
    time : str or float, optional
        Walltime as 'hh:mm:ss' or as a float number of hours.
    job_name : str, optional
        SLURM job name.
    err_file : str or Path, optional
        Path for the SLURM error log file.
    out_file : str or Path, optional
        Path for the SLURM output log file.

    Returns
    -------
    str
        SLURM header as a string ready to prepend to a bash script.
    """
    if isinstance(time, float):
        time = convert_job_time(time)
    assert isinstance(time, str), "make_header called with time variable of unknown type"

    omp_threads = int(omp_threads)
    if machine == 'perl':
        machine_string = 'cpu'
        assert omp_threads <= 256
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

    header += 'umask 0007\n'
    header += f'export OMP_NUM_THREADS={omp_threads}\n\n'

    return header


def write_script(script_path, text):
    """Write text to a script file and make it executable.

    Parameters
    ----------
    script_path : str or Path
        Destination path for the script file.
    text : str
        Script content to write.
    """
    with open(script_path, 'w+') as f:
        f.write(text)

    make_file_executable(script_path)


def run_job(script, dependency_ids=None, no_submit=False):
    """Submit a SLURM script with optional job dependencies.

    Parameters
    ----------
    script : str or Path
        Path to the SLURM script to submit.
    dependency_ids : int or list of int, optional
        SLURM job IDs that must complete successfully before this job starts.
    no_submit : bool, optional
        If True, print the sbatch command without submitting.

    Returns
    -------
    int or None
        SLURM job ID, or None if no_submit is True.
    """
    dependency = ""
    if isinstance(dependency_ids, int) and dependency_ids > 0:
        dependency = f"--dependency=afterok:{dependency_ids} "
    elif isinstance(dependency_ids, list) and len(dependency_ids) > 0:
        valid_deps = [str(j) for j in dependency_ids if (j is not None and j > 0)]
        if valid_deps:
            dependency = f"--dependency=afterok:{':'.join(valid_deps)} "

    command = f"sbatch {dependency}{script}"

    jobid = None
    if not no_submit:
        print(f'Submitting script {script}')
        process = run(command + " | tr -dc '0-9'", shell=True, capture_output=True)

        if process.returncode != 0:
            raise ValueError(f'Running "sbatch {dependency}{script}" returned non-zero exitcode '
                             f'with error {process.stderr}')

        try:
            jobid = int(process.stdout)
        except ValueError:
            raise ValueError(f'Error getting jobid from output: {process.stdout}')
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


def set_umask(mask: str = '0007') -> None:
    """Sets the value of umask.

    Parameters
    ----------
    mask : str, optional
        mask value, by default '0007'
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
    """Resolve a path that may be absolute or relative to the lyatools package or tests directory.

    Parameters
    ----------
    path : str
        Input path; absolute, or relative to the lyatools package, tests/, or repo root.
    enforce : bool, optional
        If True, raise RuntimeError when the path cannot be found; otherwise warn and return as-is.

    Returns
    -------
    Path
        Resolved absolute path.
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


def append_string_to_correlation_path(path, string):
    """Insert a string before the .fits.gz extension of a correlation file path.

    Parameters
    ----------
    path : Path
        Path to the correlation file (must end with .fits.gz).
    string : str
        String to insert before the extension (e.g. '-exp', '_shuffled').

    Returns
    -------
    Path
        New path with the string inserted before .fits.gz.
    """
    # This assumes that correlations are .fits.gz files, which should be the case by construction
    corr_name_replace = path.name.replace('.fits.gz',f'{string}.fits.gz')
    return path.parents[0] / corr_name_replace
