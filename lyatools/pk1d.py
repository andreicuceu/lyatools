from . import submit_utils

def make_pk1d_runs(analysis_tree, config, job, delta_job_ids=None):
    submit_utils.set_umask()
    job_ids = []
    id = run_picca_pk1d(
        analysis_tree, config, job, region_name='lya', delta_job_ids=delta_job_ids
    )
    job_ids += [id]

    if config.getboolean('run_lyb'):
        id = run_picca_pk1d(
            analysis_tree, config, job, region_name='lyb', delta_job_ids=delta_job_ids
        )
        job_ids += [id]
    return job_ids

def run_picca_pk1d(analysis_tree, config, job, region_name='lya',delta_job_ids=None):
    slurm_hours = config.getfloat(f'pk1d_{region_name}_slurm_hours', 0.5)

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), job.get('slurm_queue'), 
        time=slurm_hours, omp_threads=2, job_name=f'picca_pk1d_{region_name}',
        err_file=analysis_tree.logs_dir/f'picca_pk1d_{region_name}-%j.err',
        out_file=analysis_tree.logs_dir/f'picca_pk1d_{region_name}-%j.out'
    )

    if region_name == 'lya':
        in_dir = analysis_tree.deltas_lya_dir / 'Delta'
        out_dir = analysis_tree.pk1d_lya_dir
    elif region_name == 'lyb':
        in_dir = analysis_tree.deltas_lyb_dir / 'Delta'
        out_dir = analysis_tree.pk1d_lyb_dir
    else:
        raise ValueError('Unkown region name. Choose from ["lya", "lyb"].')
    
    check_file_indir = list(out_dir.glob("mean_Pk1d_*.fits.gz"))
    if len(check_file_indir) > 0:
        print(f'Pk1D output files already exist in {out_dir}, skipping Pk1D computation.')
        return None


    env_command = job.get('env_command')
    snr_min = config.getfloat('SNR_min', 1)
    lambda_obs_min = config.getfloat('lambda_obs_min', 3600)
    num_chunks = config.getint('num_chunks', 3)
    num_max_pixel_mask = config.getint('num_max_masked_pixels', 120)
    noise_estimate = config.get('noise_estimate', 'pipeline')
    nproc = config.getint('nproc', 128)
    num_noise_exp = config.getint('num_noise_exp', 2500)
    weight_method = job.get('weight_method', 'no_weights')
    rebin_factor = job.getint('rebin_factor', 3)
    num_bootstrap = job.getint('num_bootstrap', 50)
    compute_covariance = job.getboolean('compute_covariance', False)

    text = header
    text += f'{env_command}\n\n'

    text += 'echo "Starting picca_Pk1D.py"\n'
    text += f'srun picca_Pk1D.py --in-dir {in_dir} --out-dir {out_dir} '
    text += f'--lambda-obs-min {lambda_obs_min} '
    text += f'--SNR-min {snr_min} '
    text += f'--noise-estimate {noise_estimate} '
    text += f'--nb-part {num_chunks} '
    text += f'--nb-pixel-masked-max {num_max_pixel_mask} '
    text += f'--num-noise-exp {num_noise_exp} '
    text += f'--num-processors {nproc} '
    text += '\n\n'
    text += f'echo "Starting post-processing"\n'

    text += f'srun picca_Pk1D_postprocess.py --in-dir {out_dir} '
    text += f'--weight-method {weight_method} '
    text += f'--rebinfac {rebin_factor} '
    if compute_covariance:
        text += f'--covariance '
        if num_bootstrap > 0:
            text += f'--bootstrap --nbootstrap {num_bootstrap} '
    text += f'--ncpu {nproc} '

    script_path = analysis_tree.scripts_dir / f'Pk1D_{region_name}.sh'
    submit_utils.write_script(script_path, text)
    job_id = submit_utils.run_job(
        script_path, dependency_ids=delta_job_ids, no_submit=job.getboolean('no_submit'))
    return job_id



    


    


    
