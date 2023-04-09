from . import submit_utils, dir_handlers


def make_raw_deltas(input_dir, zcat_file, analysis_struct, job, zcat_job_id=None,
                    run_lyb_region=False, delta_lambda=0.8, max_num_spec=None,
                    use_old_weights=False):
    job_ids = []
    id = run_raw_deltas(input_dir, zcat_file, analysis_struct, job, zcat_job_id=zcat_job_id,
                        region_name='lya', lambda_rest_min=1040., lambda_rest_max=1200.,
                        delta_lambda=delta_lambda, max_num_spec=max_num_spec,
                        use_old_weights=use_old_weights)
    job_ids += [id]

    if run_lyb_region:
        id = run_raw_deltas(input_dir, zcat_file, analysis_struct, job,
                            zcat_job_id=zcat_job_id, region_name='lyb',
                            lambda_rest_min=920., lambda_rest_max=1020.,
                            delta_lambda=delta_lambda, max_num_spec=max_num_spec,
                            use_old_weights=use_old_weights)
        job_ids += [id]

    return job_ids


def run_raw_deltas(input_dir, zcat_file, analysis_struct, job, zcat_job_id=None,
                   region_name='lya', lambda_rest_min=1040., lambda_rest_max=1200.,
                   delta_lambda=0.8, max_num_spec=None, use_old_weights=False):
    if region_name == 'lya':
        deltas_dir = analysis_struct.deltas_lya_dir
    elif region_name == 'lyb':
        deltas_dir = analysis_struct.deltas_lyb_dir
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')

    deltas_output_dir = deltas_dir / 'Delta'
    dir_handlers.check_dir(deltas_output_dir)

    pyscript_path = analysis_struct.scripts_dir / f'deltas_{region_name}_raw.py'

    # Make the python script
    text = '#!/usr/bin/env python\n\n'
    text += 'from picca import raw_io\n\n'
    text += 'raw_io.convert_transmission_to_deltas('
    text += f'"{zcat_file}", "{deltas_output_dir}", "{input_dir}", '
    # text += f'lambda_min={args.lambda_min}, lambda_max={args.lambda_max}, '
    text += f'lambda_min_rest_frame={lambda_rest_min}, '
    text += f'lambda_max_rest_frame={lambda_rest_max}, '
    text += f'delta_lambda={delta_lambda}, lin_spaced=True, nproc=128, '

    if (max_num_spec is not None) and (max_num_spec > 0):
        text += f'max_num_spec={max_num_spec},  '

    if use_old_weights:
        text += 'use_old_weights=True)\n\n'
    else:
        text += 'use_old_weights=False)\n\n'

    submit_utils.write_script(pyscript_path, text)

    # Make the slurm script
    run_name = f'deltas_{region_name}_raw'
    slurm_script_path = analysis_struct.scripts_dir / f'run_{run_name}.sh'

    # Make the header
    header = submit_utils.make_header(job.get('nersc_machine'), job.get('slurm_queue'),
                                      nodes=1, time=0.5, omp_threads=128, job_name=run_name,
                                      err_file=analysis_struct.logs_dir/f'{run_name}-%j.err',
                                      out_file=analysis_struct.logs_dir/f'{run_name}-%j.out')

    sh_text = header
    env_command = job.get('env_command')
    sh_text += f'{env_command}\n\n'
    sh_text += f'srun -n 1 -c 128 {pyscript_path}\n'

    submit_utils.write_script(slurm_script_path, text)

    job_id = submit_utils.run_job(slurm_script_path, dependency_ids=zcat_job_id,
                                  no_submit=job.getboolean('no_submit'))

    return job_id
