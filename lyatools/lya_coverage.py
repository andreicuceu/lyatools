"""Lya coverage of a data release: build it once (in the directory where the mocks are stored) and use it to generate the seed catalogs.

The Lya coverage (``<mocks>/coverage/lya_<release>_coverage.fits``) gives the mock Lya quasar targets the footprint, completeness and exposure times of the data
(see ``desisim.survey_release.LyaCoverage``). It is built by ``build_lya_coverage`` from the LSS randoms of the release; this module builds the job, makes sure it
is submitted only once per run (even with many mocks) and returns the job id that the ``gen_qso_catalog`` jobs must wait for.
"""
from pathlib import Path

from . import submit_utils
from .dir_handlers import check_dir

SELECTION_METHODS = ['randoms', 'tiles']

# coverage path -> SLURM job id of the build job already submitted in this run (the file is shared by all the mocks)
_SUBMITTED_COVERAGE_JOBS = {}
# coverage files already reported as found in this run (the message is printed once, not once per mock)
_REPORTED_COVERAGE = set()

# Catalogs made at the same time in the batch job (each takes 1.7 GB: 64 of them use about 110 GB of the 512 of a CPU node).
SEED_CATALOG_BATCH_SIZE = 64
# Walltime of that job. Measured on one CPU node (the master catalogs read cold from disk): a wave of 48 catalogs takes 52 s, of 64 59 s,
# of 96 88 s, of 128 107 s: above 64 the catalogs per second hardly grow (1.1 to 1.2), the disk and the cores are saturated. The job needs a
# minute or so to start. The numbers below leave a factor of about 3 for a slower disk.
SEED_CATALOG_START_MINUTES = 2.0
SEED_CATALOG_WAVE_MINUTES = 3.0
SEED_CATALOG_MIN_MINUTES = 10.0


def get_selection_method(config):
    """Selection method of the seed catalogs from the [quickquasars] section: 'randoms' (Lya coverage, default) or 'tiles' (previous method)."""
    method = config.get('selection_method', 'randoms')
    if method not in SELECTION_METHODS:
        raise ValueError(f"selection_method must be one of {SELECTION_METHODS}, got '{method}'")
    return method


def get_lya_coverage_path(qq_tree, config):
    """Path of the Lya coverage: the explicit ``lya_coverage`` key of [quickquasars], else ``coverage/lya_<release>_coverage.fits`` in the directory of the mocks."""
    explicit = config.get('lya_coverage', None)
    if explicit:
        return Path(explicit)
    release = config.get('release', 'jura')
    return qq_tree.coverage_dir / f'lya_{release}_coverage.fits'


def merge_job_ids(*ids):
    """Combine job ids (int, list of int or None) into None, a single id or a list, for a SLURM dependency."""
    flat = []
    for job_id in ids:
        if isinstance(job_id, (list, tuple)):
            flat += [j for j in job_id if j is not None]
        elif job_id is not None:
            flat.append(job_id)
    if not flat:
        return None
    return flat[0] if len(flat) == 1 else flat


def make_lya_coverage(qq_tree, config, job, prev_job_id=None, run_local=True):
    """Build and submit the job that builds the Lya coverage of the release, if the file does not exist yet.

    Parameters
    ----------
    qq_tree : QQTree
        Directory tree of one mock (gives the directory where the mocks are stored).
    config : SectionProxy
        [quickquasars] section: release, input_data_catalog, lss_randoms_dir and optionally tiles_file, lya_coverage_hours,
        lya_coverage_nproc, lya_coverage.
    job : SectionProxy
        [job_info] section.
    prev_job_id : int or list of int, optional
        Job ids that the returned dependency also contains (e.g. the LyaCoLoRe job).
    run_local : bool, optional
        If True (default) write the script and submit it; if False only return the command string.

    Returns
    -------
    int or list of int or None or str
        Job id(s) the seed catalogs must wait for (``prev_job_id`` merged with the build job); with run_local=False the command string,
        or None if there is nothing to build (previous method or file already there).
    """
    if get_selection_method(config) != 'randoms':
        return prev_job_id if run_local else None

    map_path = get_lya_coverage_path(qq_tree, config)
    if map_path.is_file():
        if str(map_path) not in _REPORTED_COVERAGE:      # once per run, not once per mock
            print(f'Found Lya coverage {map_path}. Skipping build_lya_coverage.')
            _REPORTED_COVERAGE.add(str(map_path))
        return prev_job_id if run_local else None

    release = config.get('release', 'jura')
    randoms_dir = config.get('lss_randoms_dir', None)
    data_catalog = config.get('input_data_catalog')
    if randoms_dir is None:
        raise ValueError(f'The Lya coverage {map_path} does not exist and cannot be built: set lss_randoms_dir (directory with the LSS rancomb_*dark* '
                         'files of the release) in the [quickquasars] section, or give an existing file with lya_coverage.')

    nproc = config.getint('lya_coverage_nproc', 64)
    command = f'build_lya_coverage --randoms-dir {randoms_dir} --release {release} --data-catalog {data_catalog} -o {map_path} --nproc {nproc}'
    if config.get('tiles_file', None):
        command += f" --tiles-file {config.get('tiles_file')}"
    command += '\n\n'

    if not run_local:
        return command

    if str(map_path) in _SUBMITTED_COVERAGE_JOBS:
        return merge_job_ids(prev_job_id, _SUBMITTED_COVERAGE_JOBS[str(map_path)])

    submit_utils.set_umask()
    scripts_dir = qq_tree.coverage_dir / 'scripts'
    runfiles_dir = qq_tree.coverage_dir / 'run_files'
    for directory in (qq_tree.coverage_dir, scripts_dir, runfiles_dir):
        check_dir(directory)

    header = submit_utils.make_header(
        job.get('nersc_machine'), 'regular', 1, time=config.getfloat('lya_coverage_hours', 0.5),
        omp_threads=1, job_name=f'lya_coverage_{release}',
        err_file=runfiles_dir / 'build_lya_coverage-%j.err', out_file=runfiles_dir / 'build_lya_coverage-%j.out'
    )
    env_text = '\n\n' + (job.get('desi_env_command', None) or 'source /global/common/software/desi/desi_environment.sh master') + '\n\n'
    script_path = scripts_dir / f'build_lya_coverage_{release}.sh'
    submit_utils.write_script(script_path, header + env_text + command)
    map_job_id = submit_utils.run_job(script_path, dependency_ids=None, no_submit=job.getboolean('no_submit'))
    _SUBMITTED_COVERAGE_JOBS[str(map_path)] = map_job_id
    return merge_job_ids(prev_job_id, map_job_id)


def seed_catalog_hours(n_catalogs, batch_size=SEED_CATALOG_BATCH_SIZE):
    """Walltime [hours] of the batch job of the seed catalogs: it grows with the number of waves of ``batch_size`` catalogs.

    For example 10 mocks 10 min, 300 mocks (5 waves) 17 min, 1000 mocks (16 waves) 50 min.
    """
    waves = -(-n_catalogs // batch_size)
    return max(SEED_CATALOG_MIN_MINUTES, SEED_CATALOG_START_MINUTES + waves * SEED_CATALOG_WAVE_MINUTES) / 60.


def run_seed_catalog_batch(commands, qq_tree, job, prev_job_id=None, batch_size=SEED_CATALOG_BATCH_SIZE, hours=None):
    """Run many ``gen_qso_catalog`` commands at the same time in ONE job.

    Each catalog takes about 30 s alone (about 50 s with 64 running together) and 1.7 GB on one core and does not depend on the others
    (only on the Lya coverage), so a node (128 cores, 512 GB) runs 64 at once, about a minute per wave, instead of one queue wait per seed.

    Parameters
    ----------
    commands : list of str
        ``gen_qso_catalog`` commands (as returned by ``create_qq_catalog(..., run_local=False)``).
    qq_tree : QQTree
        Directory tree of one mock (gives the directory for the script and the logs).
    job : SectionProxy
        [job_info] section.
    prev_job_id : int or list of int, optional
        Jobs to wait for (LyaCoLoRe, Lya coverage).
    batch_size : int, optional
        Number of catalogs running at the same time (memory: about 2 GB each).
    hours : float, optional
        Walltime of the job. Default: ``seed_catalog_hours`` (grows with the number of catalogs).

    Returns
    -------
    int or None
        Job id of the batch job (None if there is nothing to run or nothing was submitted).
    """
    commands = [c.strip() for c in commands if c and c.strip()]
    if not commands:
        return prev_job_id
    submit_utils.set_umask()
    scripts_dir = qq_tree.version_dir / 'scripts'
    runfiles_dir = qq_tree.version_dir / 'run_files'
    for directory in (scripts_dir, runfiles_dir):
        check_dir(directory)

    header = submit_utils.make_header(
        job.get('nersc_machine'), 'regular', 1, time=hours if hours is not None else seed_catalog_hours(len(commands), batch_size),
        omp_threads=1, job_name='qq_seed_cats',
        err_file=runfiles_dir / 'seed_catalogs-%j.err', out_file=runfiles_dir / 'seed_catalogs-%j.out'
    )
    env_text = '\n\n' + (job.get('desi_env_command', None) or 'source /global/common/software/desi/desi_environment.sh master') + '\n\n'
    body = 'status=0\n\n'
    for start in range(0, len(commands), batch_size):
        body += 'pids=()\n'
        for command in commands[start:start + batch_size]:
            body += f'{command} &\npids+=($!)\n'
        body += 'for pid in "${pids[@]}"; do wait $pid || status=1; done\n\n'
    body += 'exit $status\n'
    script_path = scripts_dir / 'run_seed_catalogs.sh'
    submit_utils.write_script(script_path, header + env_text + body)
    return submit_utils.run_job(script_path, dependency_ids=prev_job_id, no_submit=job.getboolean('no_submit'))
