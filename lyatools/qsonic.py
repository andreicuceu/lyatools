from . import submit_utils


def make_qsonic_runs(
    qso_cat, qq_tree, analysis_tree, config, job, qq_job_id=None,
    mask_dla_cat=None, mask_bal_cat=None, true_continuum=False,
):
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
    print(f'Submitting job to run QSOnic on {region_name} region')
    submit_utils.set_umask()

    if region_name == 'lya':
        deltas_dirname = analysis_tree.deltas_lya_dir
    elif region_name == 'lyb':
        deltas_dirname = analysis_tree.deltas_lyb_dir
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')

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
    text += 'source /global/cfs/projectdirs/desi/science/lya/scripts/activate_qsonic.sh \n\n'
    text += f'srun -n {config.get("num_mpi")} -c 2 qsonic-fit '
    text += f'-i {qq_tree.spectra_dir} '
    text += f'-o {deltas_dirname} '
    text += f'--catalog {qso_cat} '
    text += '--mock-analysis '
    text += '--skip-resomat '
    text += '--smoothing-scale 0 '
    text += f'--skip {config.get("min_forest_fraction")} '
    text += f'--wave1 {config.get("lambda_min")} '
    text += f'--wave2 {config.get("lambda_max")} '
    text += f'--forest-w1 {lambda_rest_min} '
    text += f'--forest-w2 {lambda_rest_max} '

    if config.getboolean("save_by_healpix"):
        text += '--save-by-hpx '
    if config.getboolean("force_stack_delta_to_zero"):
        text += '--normalize-stacked-flux '
    if config.getboolean("var_fit_eta"):
        text += '--var-fit-eta '
    if config.getboolean("var_use_cov"):
        text += '--var-use-cov '

    mask_dla_flag = config.getboolean('mask_DLAs')
    if mask_dla_flag:
        assert mask_dla_cat is not None
        print(f'Asked for DLA masking. Assuming dla mask catalog exists: {mask_dla_cat}')
        text += f'--dla-mask {mask_dla_cat} '

    # TODO Add BAL masking

    if true_continuum:
        text += '--true-continuum '

    if config.getboolean('use_fid_meanflux'):
        if region_name == 'lya':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lya'))
            text += f'--fiducial-meanflux {raw_stats_file} '
        elif region_name == 'lyb':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lyb'))
            text += f'--fiducial-meanflux {raw_stats_file} '

    if config.getboolean('use_fid_varlss'):
        if region_name == 'lya':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lya'))
            text += f'--fiducial-varlss {raw_stats_file} '
        elif region_name == 'lyb':
            raw_stats_file = submit_utils.find_path(config.get('raw_stats_file_lyb'))
            text += f'--fiducial-varlss {raw_stats_file} '

    text += '\n'

    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=qq_job_id, no_submit=job.getboolean('no_submit'))

    return job_id
