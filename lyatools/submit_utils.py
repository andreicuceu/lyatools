import os
import numpy as np
from subprocess import call


def make_header(machine='perl', queue='regular', nodes=1, omp_threads=int(128), time='01:00:00',
                job_name='run_script', err_file='run-%j.err', out_file='run-%j.out'):
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
        raise ValueError(f'make_header called with uknown machine name {machine}.'
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


def run_job(header: str, command: str, path: str, no_submit: bool = False) -> None:
    """Make a job script and run it

    Parameters
    ----------
    header : str
        Job header (output of make_header function)
    command : str
        Command for job to execute
    path : str
        Path where script will pe written
    no_submit : bool, optional
        flag for submitting the job, by default False
    """
    script_text = header + '\n' + command

    with open(path, 'w+') as script:
        script.write(script_text)

    if not no_submit:
        # Send the run script.
        _ = call(f'sbatch {script}', shell=True)


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


def make_file_executable(f: str) -> None:
    """Make a file executable

    Parameters
    ----------
    f : str
        file
    """
    call(['chmod', 'ug+x', f])
