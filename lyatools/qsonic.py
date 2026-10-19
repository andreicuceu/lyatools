from . import submit_utils
from . import dir_handlers


def make_qsonic_runs(
    qso_cat, qq_tree, analysis_tree, config, job, qq_job_id=None,
    mask_dla_cat=None, mask_bal_cat=None, true_continuum=False,
):
    """Submit QSOnic delta extraction jobs for the Lya and/or Lyb regions.

    Parameters
    ----------
    qso_cat : Path
        Path to the QSO catalog.
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    analysis_tree : AnalysisTree
        Directory tree for analysis outputs.
    config : SectionProxy
        QSOnic configuration section.
    job : SectionProxy
        Job configuration section.
    qq_job_id : int or list of int, optional
        SLURM job IDs to depend on.
    mask_dla_cat : Path, optional
        Path to the DLA mask catalog.
    mask_bal_cat : Path, optional
        Path to the BAL mask catalog (currently unused in QSOnic).
    true_continuum : bool, optional
        Whether to use the true continuum instead of fitting it.

    Returns
    -------
    list of int
        SLURM job IDs for the submitted QSOnic jobs.
    """
    job_ids = []
    if config.getboolean('run_lya_region'):
        id = run_qsonic(
            qso_cat, qq_tree, analysis_tree, config, job, qq_job_id=qq_job_id, region_name='lya',
            mask_dla_cat=mask_dla_cat, mask_bal_cat=mask_bal_cat, true_continuum=true_continuum,
            lambda_rest_min=config.getfloat('lambda_rest_lya_min'),
            lambda_rest_max=config.getfloat('lambda_rest_lya_max'),
        )
        job_ids += [id]

    if config.getboolean('run_lyb_region'):
        id = run_qsonic(
            qso_cat, qq_tree, analysis_tree, config, job, qq_job_id=qq_job_id, region_name='lyb',
            lambda_rest_min=config.getfloat('lambda_rest_lyb_min'),
            lambda_rest_max=config.getfloat('lambda_rest_lyb_max'),
        )
        job_ids += [id]

    if len(job_ids) < 1:
        raise ValueError('Asked for deltas, but turned off both lya and lyb regions.')

    return job_ids


def run_qsonic(
    qso_cat, qq_tree, analysis_tree, config, job, qq_job_id=None, region_name='lya',
    mask_dla_cat=None, mask_bal_cat=None, true_continuum=False,
    lambda_rest_min=1040., lambda_rest_max=1205.,
):
    """Build and submit a single QSOnic job for one spectral region.

    Parameters
    ----------
    qso_cat : Path
        Path to the QSO catalog.
    qq_tree : QQTree
        Directory tree for the QuickQuasars run.
    analysis_tree : AnalysisTree
        Directory tree for analysis outputs.
    config : SectionProxy
        QSOnic configuration section.
    job : SectionProxy
        Job configuration section.
    qq_job_id : int or list of int, optional
        SLURM job IDs to depend on.
    region_name : {'lya', 'lyb'}, optional
        Spectral region to process.
    mask_dla_cat : Path, optional
        Path to the DLA mask catalog.
    mask_bal_cat : Path, optional
        Path to the BAL mask catalog (currently unused).
    true_continuum : bool, optional
        Whether to use the true continuum instead of fitting it.
    lambda_rest_min : float, optional
        Minimum rest-frame wavelength in Angstroms.
    lambda_rest_max : float, optional
        Maximum rest-frame wavelength in Angstroms.

    Returns
    -------
    int or None
        SLURM job ID, or None if no_submit is True.
    """
    print(f'Submitting job to run QSOnic on {region_name} region')
    submit_utils.set_umask()

    if region_name == 'lya':
        deltas_dirname = analysis_tree.deltas_lya_dir
    elif region_name == 'lyb':
        deltas_dirname = analysis_tree.deltas_lyb_dir
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')

    deltas_dir = deltas_dirname / 'Delta'
    dir_handlers.check_dir(deltas_dir)

    # Create the path and name for the config file
    type = 'true' if true_continuum else 'fitted'
    run_name = f'qsonic_{region_name}_{type}'
    script_path = analysis_tree.scripts_dir / f'run_{run_name}.sh'

    slurm_hours = config.getfloat('slurm_hours', 0.3)

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), job.get('slurm_queue'),
        time=slurm_hours, omp_threads=int(2), job_name=run_name,
        err_file=analysis_tree.logs_dir/f'{run_name}-%j.err',
        out_file=analysis_tree.logs_dir/f'{run_name}-%j.out'
    )

    # Create the script
    text = header
    # env_command = job.get('env_command')
    text += 'source /global/cfs/projectdirs/desi/science/lya/scripts/activate_qsonic.sh\n\n'
    command_lines = []
    command_lines.append(f'srun -n {config.get("num_mpi")} -c 2 qsonic-fit')
    command_lines.append(f'-i {qq_tree.spectra_dir}')
    command_lines.append(f'-o {deltas_dir}')
    command_lines.append(f'--catalog {qso_cat}')
    command_lines.append('--mock-analysis')
    command_lines.append('--skip-resomat')
    command_lines.append('--smoothing-scale 0')
    command_lines.append(f'--skip {config.get("min_forest_fraction")}')
    command_lines.append(f'--skip-min-pixels {config.get("num_pix_min")}')
    command_lines.append(f'--eta-varlss {config.get("var_lss_mod")}')
    command_lines.append(f'--wave1 {config.get("lambda_min")}')
    command_lines.append(f'--wave2 {config.get("lambda_max")}')
    command_lines.append(f'--forest-w1 {lambda_rest_min}')
    command_lines.append(f'--forest-w2 {lambda_rest_max}')

    if config.getboolean("save_by_healpix"):
        command_lines.append('--save-by-hpx')
    if config.getboolean("force_stack_delta_to_zero"):
        command_lines.append('--normalize-stacked-flux')
    if config.getboolean("var_fit_eta"):
        command_lines.append('--var-fit-eta')
    if config.getboolean("var_use_cov"):
        command_lines.append('--var-use-cov')
    nside_opt = config.get("nside", None)
    if nside_opt is not None:
        command_lines.append(f'--nside {nside_opt}')

    mask_dla_flag = config.getboolean('mask_DLAs')
    if mask_dla_flag:
        assert mask_dla_cat is not None
        print(f'Asked for DLA masking. Assuming dla mask catalog exists: {mask_dla_cat}')
        command_lines.append(f'--dla-mask {mask_dla_cat}')
        if not config.getboolean('mask_associated_metals_dla'):
            print('Asked for NOT masking associated metals.')
            command_lines.append('--dla-dont-mask-metals')

    mask_bal_flag = config.getboolean('mask_BALs')
    if mask_bal_flag:
        if mask_bal_cat is None:
            print('Asked for BAL masking but did not provide a mask catalog.'
                  'Assuming BAL catalog is in the quasar catalog.')
            command_lines.append('--bal-mask')
        else:
            print(f'Asked for BAL masking. Assuming bal mask catalog exists: {mask_bal_cat}')
            command_lines.append(f'--bal-mask-catalog {mask_bal_cat}')

    if true_continuum:
        command_lines.append('--true-continuum')

    if config.getboolean('use_fid_meanflux'):
        if region_name == 'lya':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lya'))
            command_lines.append(f'--fiducial-meanflux {raw_stats_file}')
        elif region_name == 'lyb':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lyb'))
            command_lines.append(f'--fiducial-meanflux {raw_stats_file}')

    if config.getboolean('use_fid_varlss'):
        if region_name == 'lya':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lya'))
            command_lines.append(f'--fiducial-varlss {raw_stats_file}')
        elif region_name == 'lyb':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lyb'))
            command_lines.append(f'--fiducial-varlss {raw_stats_file}')

    text += ' \\\n'.join(command_lines) + '\n'

    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=qq_job_id, no_submit=job.getboolean('no_submit'))

    return job_id
