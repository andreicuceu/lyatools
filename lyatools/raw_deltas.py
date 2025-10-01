from . import submit_utils, dir_handlers

LYA_TRANSMISSION_HDUNAME = {
    'lyacolore': 'F_LYA',
    'saclay': 'TRANSMISSION'
}


def make_raw_deltas(qso_cat, skewers_path, analysis_tree, config, mock_type, job, qq_job_id=None):
    job_ids = []
    if config.getboolean('run_lya_region'):
        id = run_raw_deltas(
            qso_cat, skewers_path, analysis_tree, config, mock_type, job,
            qq_job_id=qq_job_id, region_name='lya',
            lambda_rest_min=config.getfloat('lambda_rest_lya_min'),
            lambda_rest_max=config.getfloat('lambda_rest_lya_max'),
        )
        job_ids += [id]

    if config.getboolean('run_lyb_region'):
        id = run_raw_deltas(
            qso_cat, skewers_path, analysis_tree, config, mock_type, job,
            qq_job_id=qq_job_id, region_name='lyb',
            lambda_rest_min=config.getfloat('lambda_rest_lyb_min'),
            lambda_rest_max=config.getfloat('lambda_rest_lyb_max'),
        )
        job_ids += [id]

    if len(job_ids) < 1:
        raise ValueError('Asked for deltas, but turned off both lya and lyb regions.')

    return job_ids


def run_raw_deltas(
        qso_cat, skewers_path, analysis_tree, config, mock_type, job, qq_job_id=None,
        region_name='lya', lambda_rest_min=1040, lambda_rest_max=1200,
):
    if region_name == 'lya':
        deltas_dir = analysis_tree.deltas_lya_dir
    elif region_name == 'lyb':
        deltas_dir = analysis_tree.deltas_lyb_dir
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')

    deltas_output_dir = deltas_dir / 'Delta'
    dir_handlers.check_dir(deltas_output_dir)

    # Get the parameters we need
    lambda_min = config.getfloat('lambda_min')
    lambda_max = config.getfloat('lambda_max')
    delta_lambda = config.getfloat('delta_lambda')
    nproc = config.getint('nproc', 64)
    max_num_spec = config.getint('max_num_spec')
    use_old_weights = config.getboolean('use_old_weights')

    pyscript_path = analysis_tree.scripts_dir / f'deltas_{region_name}_raw.py'

    # Make the python script
    text = '#!/usr/bin/env python\n\n'
    text += 'from picca import raw_io\n\n'
    text += 'raw_io.convert_transmission_to_deltas('
    text += f'"{qso_cat}", "{deltas_output_dir}", "{skewers_path}", '
    text += f'lambda_min={lambda_min}, lambda_max={lambda_max}, '
    text += f'lambda_min_rest_frame={lambda_rest_min}, '
    text += f'lambda_max_rest_frame={lambda_rest_max}, '
    text += f'delta_lambda={delta_lambda}, lin_spaced=True, nproc={nproc}, use_splines=True, '
    text += f'tracer="{LYA_TRANSMISSION_HDUNAME[mock_type]}", '

    if (max_num_spec is not None) and (max_num_spec > 0):
        text += f'max_num_spec={max_num_spec},  '

    if use_old_weights:
        text += 'use_old_weights=True)\n\n'
    else:
        text += 'use_old_weights=False)\n\n'

    submit_utils.write_script(pyscript_path, text)

    # Make the slurm script
    run_name = f'deltas_{region_name}_raw'
    slurm_script_path = analysis_tree.scripts_dir / f'run_{run_name}.sh'

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), job.get('slurm_queue'),
        nodes=1, time=0.5, omp_threads=128, job_name=run_name,
        err_file=analysis_tree.logs_dir/f'{run_name}-%j.err',
        out_file=analysis_tree.logs_dir/f'{run_name}-%j.out'
    )

    sh_text = header
    env_command = job.get('env_command')
    sh_text += f'{env_command}\n\n'
    sh_text += f'srun -n 1 -c 128 {pyscript_path}\n'

    submit_utils.write_script(slurm_script_path, sh_text)

    job_id = submit_utils.run_job(
        slurm_script_path, dependency_ids=qq_job_id, no_submit=job.getboolean('no_submit'))

    return job_id
