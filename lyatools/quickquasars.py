from . import submit_utils
from lyatools import qq_run_args


def create_qq_catalog(qq_tree, seed_cat_path, config, job, seed, prev_job_id=None, run_local=True):
    """Build and submit a job to generate the seeded QSO input catalog for QuickQuasars.

    Parameters
    ----------
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    seed_cat_path : Path
        Output path for the seed catalog file.
    config : SectionProxy
        QuickQuasars configuration section.
    job : SectionProxy
        Job configuration section.
    seed : int
        Catalog seed used by gen_qso_catalog.
    prev_job_id : int or list of int, optional
        SLURM job IDs to depend on.
    run_local : bool, optional
        If True, submit a SLURM job; if False, return the command string.

    Returns
    -------
    int or str or None
        SLURM job ID, command string (run_local=False), or None if catalog exists.
    """
    submit_utils.set_umask()

    release = config.get('release', 'jura')
    master_cat_path = f"{qq_tree.skewers_path}/master.fits"
    input_data_catalog = config.get('input_data_catalog')

    command = f'gen_qso_catalog -i {master_cat_path} -o {seed_cat_path} --seed {seed}'
    command += f' --release {release} --input-data {input_data_catalog}'
    if config.getboolean('invert_cat_seed', False):
        command += ' --invert'
    if config.getboolean('include_nonqso_targets', True):
        command += ' --include-nonqso-targets'
    command += '\n\n'

    if not run_local:
        return command

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), 'regular', 1, time=0.2,
        omp_threads=128, job_name=f'qq_cat_{seed}',
        err_file=qq_tree.runfiles_dir/'run-%j.err',
        out_file=qq_tree.runfiles_dir/'run-%j.out'
    )

    env_text = '\n\n'
    if job.get('desi_env_command', None) is None:
        env_text += 'source /global/common/software/desi/desi_environment.sh master'
    else:
        env_text += job.get('desi_env_command')
    env_text += '\n\n'

    # Write the script to file and run it.
    script_path = qq_tree.scripts_dir / 'run_qq_seed_cat.sh'
    full_text = header + env_text + command

    job_id = None
    if not seed_cat_path.is_file():
        submit_utils.write_script(script_path, full_text)
        job_id = submit_utils.run_job(
            script_path, dependency_ids=prev_job_id, no_submit=job.getboolean('no_submit'))

    return job_id


def run_qq(qq_tree, config, job, seed_cat_path, qq_seed, qq_special_args, prev_job_id=None):
    """Build and submit the QuickQuasars SLURM job for a single mock seed.

    Parameters
    ----------
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    config : SectionProxy
        QuickQuasars configuration section.
    job : SectionProxy
        Job configuration section.
    seed_cat_path : Path
        Path to the seeded QSO input catalog.
    qq_seed : int
        Random seed passed to quickquasars.
    qq_special_args : list of str
        Extra CLI arguments derived from the qq_run_type digit codes.
    prev_job_id : int or list of int, optional
        SLURM job IDs to depend on.

    Returns
    -------
    int or None
        SLURM job ID, or None if no_submit is True.
    """
    # Print run config
    print(f'Submitting quickquasars runs with configuration {qq_tree.qq_run_name}')

    qq_args = ' '.join(qq_run_args.QQ_DEFAULTS)
    qq_args += f' --seed {qq_seed}'
    qq_args += f' --from-catalog {seed_cat_path} '
    if len(qq_special_args) > 0:
        qq_args += ' '.join(qq_special_args)

    print('Found the following arguments to pass to quickquasars:')
    print(qq_args)

    qq_script = create_qq_script(qq_tree, config, job, qq_args, qq_seed)

    job_id = submit_utils.run_job(
        qq_script, dependency_ids=prev_job_id, no_submit=job.getboolean('no_submit'))

    return job_id


def create_qq_script(qq_tree, config, job, qq_args, qq_seed):
    """Write the QuickQuasars SLURM bash script to disk and return its path.

    Parameters
    ----------
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    config : SectionProxy
        QuickQuasars configuration section.
    job : SectionProxy
        Job configuration section.
    qq_args : str
        Assembled CLI argument string to pass to quickquasars.
    qq_seed : int
        Random seed (used in the job name).

    Returns
    -------
    Path
        Path to the written script file.
    """
    submit_utils.set_umask()

    slurm_queue = job.get('slurm_queue', 'regular')
    nodes = config.getint('nodes', 8)
    nproc = config.getint('nproc', 32)
    slurm_hours = config.getfloat('slurm_hours', 0.5)

    # Make the header
    time = submit_utils.convert_job_time(slurm_hours)
    header = submit_utils.make_header(
        job.get('nersc_machine'), slurm_queue, nodes, time=time,
        omp_threads=nproc, job_name=f'qq_{qq_seed}',
        err_file=qq_tree.runfiles_dir/'run-%j.err',
        out_file=qq_tree.runfiles_dir/'run-%j.out'
    )

    # Create the main qq run command
    qq_run = f'    command="srun -N 1 -n 1 -c {nproc} '
    qq_run += f'quickquasars -i $tfiles --nproc {nproc} '
    qq_run += f'--outdir {qq_tree.spectra_dir} {qq_args}"\n'

    # Make the text body of the script.
    text = '\n\n'
    if job.get('desi_env_command', None) is None:
        text += 'source /global/common/software/desi/desi_environment.sh master'
    else:
        text += job.get('desi_env_command')

    text += '\n\n'
    text += 'echo "get list of skewers to run ..."\n\n'

    if job.getboolean('test_run'):
        text += 'echo "test run enabled, selecting only first 10 files"\n'
        text += f'files=`ls -1 {qq_tree.skewers_path}/*/*/transmission*.fits* | head -10`\n'
    else:
        text += f'files=`ls -1 {qq_tree.skewers_path}/*/*/transmission*.fits*`\n'

    text += 'nfiles=`echo $files | wc -w`\n'
    text += f'nfilespernode=$(( $nfiles / {nodes} + 1 ))\n\n'
    text += 'echo "n files =" $nfiles\n'
    text += 'echo "n files per node =" $nfilespernode\n\n'

    text += 'first=1\n'
    text += 'last=$nfilespernode\n'
    text += f'for node in `seq {nodes}` ; do\n'
    text += '    echo "starting node $node"\n\n'
    text += '    # list of files to run\n'
    text += f'    if (( $node == {nodes} )) ; then\n'
    text += '        last=""\n'
    text += '    fi\n\n'
    text += '    echo ${first}-${last}\n'
    text += '    tfiles=`echo $files | cut -d " " -f ${first}-${last}`\n'
    text += '    first=$(( first + nfilespernode ))\n'
    text += '    last=$(( last + nfilespernode ))\n'

    text += qq_run

    text += '    echo $command\n'
    text += f'    echo "log in {qq_tree.logs_dir}/node-$node.log"\n\n'
    text += f'    $command >& {qq_tree.logs_dir}/node-$node.log &\n\n'
    text += 'done\n\n'
    text += 'wait\n\n'

    text += 'echo "END"\n\n'

    full_text = header + text

    # Write the script to file and run it.
    script_path = qq_tree.scripts_dir / 'run_quickquasars.sh'
    submit_utils.write_script(script_path, full_text)

    return script_path


def make_catalogs(qq_tree, config, job, dla_flag, bal_flag, qq_job_id, only_qso_targets):
    """Submit jobs to build the QSO, SNR, BAL, and DLA catalogs after a QuickQuasars run.

    Parameters
    ----------
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    config : SectionProxy
        QuickQuasars configuration section.
    job : SectionProxy
        Job configuration section.
    dla_flag : bool
        Whether to build the DLA catalog.
    bal_flag : bool
        Whether to build the BAL catalog.
    qq_job_id : int or None
        SLURM job ID of the QuickQuasars job to depend on.
    only_qso_targets : bool
        Whether to restrict the QSO catalog to QSO-targeted objects.

    Returns
    -------
    int or None
        SLURM job ID of the last catalog job submitted.
    """
    job_id = qq_job_id

    # Check which QSO catalog to use
    command = ''
    zcat_file = qq_tree.qq_dir / 'zcat.fits'
    if only_qso_targets:
        zcat_file = qq_tree.qq_dir / 'zcat_only_qso_targets.fits'
    print(f"Only QSO Targets flag: {only_qso_targets}")
    print(f"Expected zcat_file path: {zcat_file}")

    # Create the QSO catalog command
    if not zcat_file.is_file():
        only_qso_targets_flag = "--only_qso_targets" if only_qso_targets else ""
        command += f'lyatools-make-zcat -i {qq_tree.spectra_dir} -o {zcat_file}'
        command += f' --nproc {128} {only_qso_targets_flag}\n\n'

    # Create the BAL catalog command
    bal_cat_check = qq_tree.qq_dir / 'bal_cat.fits'
    if bal_flag and not bal_cat_check.is_file():
        command += f'lyatools-make-bal-cat -i {qq_tree.spectra_dir} -o {qq_tree.qq_dir} '

        ai_cut = config.getint('bal_ai_cut', None)
        if ai_cut is not None:
            command += f'--ai-cut {ai_cut} '

        bi_cut = config.getint('bal_bi_cut', None)
        if bi_cut is not None:
            command += f'--bi-cut {bi_cut} '

        command += f'--nproc {128}\n\n'

    # Submit the QSO/BAL catalog job
    if len(command) > 0:
        print('Submitting QSO/BAL catalog job')
        job_id = run_cat_job(command, 'qso_bal', qq_tree, job, job_id)

    # Run SNR catalog job
    snr_cat = qq_tree.qq_dir / 'snr_cat.fits'
    if not snr_cat.is_file():
        job_id = snr_cat_job(snr_cat, qq_tree, bal_flag, job, job_id)

    # Run DLA catalog job
    dla_cat_check = qq_tree.qq_dir / 'dla_cat.fits'
    mask_nhi_cut = config.getfloat('dla_mask_nhi_cut')
    mask_snr_cut = config.getfloat('dla_mask_snr_cut')
    completeness = config.getfloat('dla_completeness')

    dla_cat_name = f'dla_cat_nhi_{mask_nhi_cut:.2f}_snr_{mask_snr_cut:.1f}'
    dla_cat_name += f'_completeness_{completeness:.2f}.fits'
    dla_cat_check2 = qq_tree.qq_dir / dla_cat_name
    dla_cat_exist = dla_cat_check.is_file() and dla_cat_check2.is_file()
    if dla_flag and not dla_cat_exist:
        command = f'lyatools-make-dla-cat -i {qq_tree.spectra_dir} -o {qq_tree.qq_dir} '
        command += f'--mask-nhi-cut {mask_nhi_cut} --mask-snr-cut {mask_snr_cut} '

        nhi_errors = config.getfloat('dla_nhi_errors', None)
        if nhi_errors is not None:
            command += f'--nhi-errors {nhi_errors} '
        command += f'--completeness {completeness} --seed {qq_tree.mock_seed} --nproc {128}\n\n'

        print('Submitting DLA catalog job')
        job_id = run_cat_job(command, 'dla', qq_tree, job, job_id)

    return job_id


def run_cat_job(command, name, qq_tree, job, job_id):
    """Write and submit a catalog-building SLURM job.

    Parameters
    ----------
    command : str
        Shell command to run inside the SLURM script.
    name : str
        Label for the job (used in script filename and job name).
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    job : SectionProxy
        Job configuration section.
    job_id : int or None
        SLURM job ID to depend on.

    Returns
    -------
    int or None
        SLURM job ID, or None if no_submit is True.
    """
    header = submit_utils.make_header(
        job.get('nersc_machine'), nodes=1, time=0.5,
        omp_threads=128, job_name=f'{name}_{qq_tree.mock_seed}',
        err_file=qq_tree.runfiles_dir/f'run-{name}-%j.err',
        out_file=qq_tree.runfiles_dir/f'run-{name}-%j.out'
    )

    env_command = job.get('env_command')
    text = header + f'{env_command}\n\n' + command

    script_path = qq_tree.scripts_dir / f'make_{name}_cat.sh'
    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=job_id, no_submit=job.getboolean('no_submit'))

    return job_id


def snr_cat_job(snr_cat, qq_tree, bal_flag, job, cat_job_id):
    """Write and submit a SLURM job to build the SNR catalog.

    Parameters
    ----------
    snr_cat : Path
        Output path for the SNR catalog file.
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    bal_flag : bool
        Whether to apply BAL masking when computing SNR.
    job : SectionProxy
        Job configuration section.
    cat_job_id : int or None
        SLURM job ID to depend on.

    Returns
    -------
    int or None
        SLURM job ID, or None if no_submit is True.
    """
    script = submit_utils.find_path('lyatools/scripts/make_snr_cat.py', enforce=True)
    command = f'python {script} --path {qq_tree.qq_dir} -o {snr_cat} '
    if bal_flag:
        command += '--balmask '

    command += f'--nproc {128}\n\n'

    header = submit_utils.make_header(
        job.get('nersc_machine'), nodes=1, time=0.5,
        omp_threads=128, job_name=f'snr_cat_{qq_tree.mock_seed}',
        err_file=qq_tree.runfiles_dir/'run-snr-cat-%j.err',
        out_file=qq_tree.runfiles_dir/'run-snr-cat-%j.out'
    )

    if job.get('desi_env_command', None) is None:
        env_command = 'source /global/common/software/desi/desi_environment.sh master'
    else:
        env_command = job.get('desi_env_command')

    text = header + f'{env_command}\n\n' + command

    script_path = qq_tree.scripts_dir / 'make_snr_cat.sh'
    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=cat_job_id, no_submit=job.getboolean('no_submit'))

    return job_id
