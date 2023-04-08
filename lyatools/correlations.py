from . import submit_utils

JOB_CONFIGS = {'cf_lya_lya': 2.0, 'dmat_lya_lya': 2.0,
               'cf_lya_lyb': 1.2, 'dmat_lya_lyb': 1.2,
               'xcf_lya_qso': 0.5, 'xdmat_lya_qso': 0.5,
               'xcf_lyb_qso': 0.25, 'xdmat_lyb_qso': 0.25}


def make_correlation_runs(config, job, analysis_struct, corr_types, catalogue, delta_job_ids=None):
    submit_utils.set_umask()
    job_ids = []

    no_comput_corr = config.getboolean('no_compute_corr')
    compute_dmat = config.getboolean('compute_dmat')

    if 'lya_lya' in corr_types:
        if not no_comput_corr:
            job_ids.append(run_correlation(config, job, analysis_struct, name='cf_lya_lya',
                                           delta_job_ids=delta_job_ids))
        if compute_dmat:
            job_ids.append(run_correlation(config, job, analysis_struct, dmat=True,
                                           name='dmat_lya_lya', delta_job_ids=delta_job_ids))

    if 'lya_lyb' in corr_types:
        if not no_comput_corr:
            job_ids.append(run_correlation(config, job, analysis_struct, lyb=True,
                                           name='cf_lya_lyb', delta_job_ids=delta_job_ids))
        if compute_dmat:
            job_ids.append(run_correlation(config, job, analysis_struct, lyb=True, dmat=True,
                                           name='dmat_lya_lyb', delta_job_ids=delta_job_ids))

    if 'lya_qso' in corr_types:
        if not no_comput_corr:
            job_ids.append(run_correlation(config, job, analysis_struct, catalogue, cross=True,
                                           name='xcf_lya_qso', delta_job_ids=delta_job_ids))
        if compute_dmat:
            job_ids.append(run_correlation(config, job, analysis_struct, catalogue, cross=True,
                                           dmat=True, name='xdmat_lya_qso',
                                           delta_job_ids=delta_job_ids))

    if 'lyb_qso' in corr_types:
        if not no_comput_corr:
            job_ids.append(run_correlation(config, job, analysis_struct, catalogue, cross=True,
                                           lyb=True, name='xcf_lyb_qso',
                                           delta_job_ids=delta_job_ids))
        if compute_dmat:
            job_ids.append(run_correlation(config, job, analysis_struct, catalogue, cross=True,
                                           lyb=True, dmat=True, name='xdmat_lyb_qso',
                                           delta_job_ids=delta_job_ids))

    return job_ids


def run_correlation(config,  job, analysis_struct, catalogue=None, cross=False, lyb=False,
                    dmat=False, name='cf_lya_lya', delta_job_ids=None):
    slurm_hours = config.getfloat(f'{name}_slurm_hours', None)
    if slurm_hours is None:
        slurm_hours = JOB_CONFIGS[name]

    nproc = job.getint('nproc')
    # Make the header
    header = submit_utils.make_header(job.get('nersc_machine'), job.get('slurm_queue'),
                                      time=slurm_hours, omp_threads=nproc, job_name=name,
                                      err_file=analysis_struct.logs_dir/f'{name}-%j.err',
                                      out_file=analysis_struct.logs_dir/f'{name}-%j.out')

    # TODO implement other options for redshift bins
    zmin, zmax = 0, 10
    script_type = name.split('_')[0]
    name_string = config.get('name_string', None)
    if name_string is None:
        output_path = analysis_struct.corr_dir / f'{name}_{zmin}_{zmax}.fits.gz'
    else:
        output_path = analysis_struct.corr_dir / f'{name}_{zmin}_{zmax}_{name_string}.fits.gz'

    # Get setting we need
    env_command = job.get('env_command')
    rp_min = config.getfloat('rp_min')
    rp_max = config.getfloat('rp_max')
    rt_max = config.getfloat('rt_max')
    num_bins_rp = config.getint('num_bins_rp')
    num_bins_rt = config.getint('num_bins_rt')
    fid_Om = config.getfloat('fid_Om')
    dmat_rejection = config.getfloat('dmat_rejection')
    rebin_factor = config.getint('rebin_factor', None)

    # Create the script
    text = header
    text += f'{env_command}\n\n'
    text += f'srun -n 1 -c {nproc} picca_{script_type}.py '
    text += f'--out {output_path} '

    if cross and lyb:
        text += f'--in-dir {analysis_struct.deltas_lyb_dir} '
    else:
        text += f'--in-dir {analysis_struct.deltas_lya_dir} '

    if lyb and not cross:
        text += f'--in-dir2 {analysis_struct.deltas_lyb_dir} '

    if cross:
        text += f'--drq {catalogue} --mode desi_mocks --z-evol-obj 1.44 --rp-min -200 '
    else:
        text += f'--rp-min {rp_min} '

    text += f'--rp-max {rp_max} --rt-max {rt_max} --nt {num_bins_rt} '
    if cross:
        text += f'--np {2*num_bins_rp} '
    else:
        text += f'--np {num_bins_rp} '

    text += f'--z-cut-min {zmin} --z-cut-max {zmax} --fid-Om {fid_Om} --nproc {nproc} '
    text += '--fid-Or 7.97505418919554e-05 '

    if config.getboolean('no_project'):
        text += '--no-project '

    if dmat:
        text += f'--rej {dmat_rejection} '

    if cross and config.getboolean('no_remove_mean_lambda_obs'):
        text += '--no-remove-mean-lambda-obs '

    if rebin_factor is not None:
        text += f'--rebin-factor {rebin_factor} '

    text += '\n\n'

    script_path = analysis_struct.scripts_dir / f'{name}_{zmin}_{zmax}.sh'

    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(script_path, dependency_ids=delta_job_ids,
                                  no_submit=config.getboolean('no_submit'))

    return job_id
