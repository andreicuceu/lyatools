from . import submit_utils

JOB_CONFIGS = {'cf_lya_lya': 1.5, 'dmat_lya_lya': 2.0, 'metal_dmat_lya_lya': 2.0,
               'cf_lya_lyb': 1.0, 'dmat_lya_lyb': 1.0, 'metal_dmat_lya_lyb': 1.0,
               'xcf_lya_qso': 0.5, 'xdmat_lya_qso': 0.5, 'metal_xdmat_lya_qso': 0.5,
               'xcf_lyb_qso': 0.25, 'xdmat_lyb_qso': 0.25, 'metal_xdmat_lyb_qso': 0.25}

CORR_TYPES = ['lya_lya', 'lya_lyb', 'lya_qso', 'lyb_qso']


def make_correlation_runs(qso_cat, analysis_tree, config, job, corr_types, delta_job_ids=None):
    submit_utils.set_umask()
    cf_out = []
    cf_shuffled_out = []
    dmat_out = []
    metal_out = []

    no_comput_corr = config.getboolean('no_compute_corr')
    compute_dmat = config.getboolean('compute_dmat')
    compute_metals = config.getboolean('compute_metals')
    compute_shuffled = config.getboolean('compute_shuffled')

    for corr in corr_types:
        assert corr in CORR_TYPES
        lyb = 'lyb' in corr
        cross = 'qso' in corr

        if not no_comput_corr:
            cf_out.append(run_correlation(
                config, job, analysis_tree, qso_cat, cross=cross, lyb=lyb,
                name=f"{'x' if cross else ''}cf_{corr}", delta_job_ids=delta_job_ids
            ))

            if compute_shuffled and cross:
                cf_shuffled_out.append(run_correlation(
                    config, job, analysis_tree, qso_cat, cross=cross, lyb=lyb,
                    name=f"{'x' if cross else ''}cf_{corr}", shuffled=True,
                    delta_job_ids=delta_job_ids
                ))

        if compute_dmat:
            dmat_out.append(run_correlation(
                config, job, analysis_tree, qso_cat, cross=cross, lyb=lyb, dmat=True,
                name=f"{'x' if cross else ''}dmat_{corr}", delta_job_ids=delta_job_ids
            ))

        if compute_metals:
            metal_out.append(run_correlation(
                config, job, analysis_tree, qso_cat, cross=cross, lyb=lyb, metal_dmat=True,
                name=f"{'x' if cross else ''}metal_dmat_{corr}", delta_job_ids=delta_job_ids
            ))

    cf_paths = [out[0] for out in cf_out]
    job_ids = [out[1] for out in cf_out] + [out[1] for out in cf_shuffled_out]
    return cf_paths, job_ids


def run_correlation(
    config, job, analysis_tree, qso_cat=None, cross=False, lyb=False,
    dmat=False, metal_dmat=False, name='cf_lya_lya', shuffled=False,
    delta_job_ids=None,
):
    slurm_hours = config.getfloat(f'{name}_slurm_hours', None)
    if slurm_hours is None:
        slurm_hours = JOB_CONFIGS[name]

    # Make the header
    header = submit_utils.make_header(
        job.get('nersc_machine'), job.get('slurm_queue'),
        time=slurm_hours, omp_threads=2, job_name=name,
        err_file=analysis_tree.logs_dir/f'{name}-%j.err',
        out_file=analysis_tree.logs_dir/f'{name}-%j.out'
    )

    z_min_default, z_max_default = 0, 10
    zmin = config.getfloat('z_min', z_min_default)
    zmax = config.getfloat('z_max', z_max_default)
    script_type = name.split('_')[0]
    if metal_dmat:
        script_type = 'metal_' + name.split('_')[1]

    name_string = config.get('name_string', None)
    rmu_binning = config.getboolean("r_mu_binning", False)
    if shuffled:
        assert not dmat and not metal_dmat
        name_string = 'shuffled' if name_string is None else f'{name_string}_shuffled'
    if rmu_binning:
        name_string = 'rmu' if name_string is None else f'{name_string}_rmu'
    gzed = '' if (dmat or metal_dmat) else '.gz'
    if name_string is None:
        output_path = analysis_tree.corr_dir / f'{name}_{zmin}_{zmax}.fits{gzed}'
    else:
        output_path = analysis_tree.corr_dir / f'{name}_{zmin}_{zmax}_{name_string}.fits{gzed}'

    if output_path.is_file():
        print(f'Correlation already exists, skipping: {output_path}.')
        return output_path, None

    # Get setting we need
    env_command = job.get('env_command')
    nside = config.getint('nside')
    rp_min = config.getfloat('rp_min')
    rp_max = config.getfloat('rp_max')
    rt_max = config.getfloat('rt_max')
    num_bins_rp = config.getint('num_bins_rp')
    num_bins_rt = config.getint('num_bins_rt')
    fid_Om = config.getfloat('fid_Om')
    fid_Or = config.getfloat('fid_Or', 7.97505418919554e-05)
    dmat_rejection = config.getfloat('dmat_rejection')
    dmat_num_bins_rp = config.getint('dmat_num_bins_rp', num_bins_rp)
    dmat_rp_max = config.getfloat('dmat_rp_max', rp_max)
    coeff_binning = config.getint('coeff_binning', None)
    rebin_factor = config.getint('rebin_factor', None)
    zerr_cut_deg = config.getfloat('zerr_cut_deg', None)
    zerr_cut_kms = config.getfloat('zerr_cut_kms', None)
    nproc = config.getint('nproc', 128)

    # Create the script
    text = header
    text += f'{env_command}\n\n'
    text += f'picca_{script_type}.py '
    text += f'--out {output_path} '

    if cross and lyb:
        in_dir = analysis_tree.deltas_lyb_dir / 'Delta'
    else:
        in_dir = analysis_tree.deltas_lya_dir / 'Delta'

    text += f'--in-dir {in_dir} '

    if lyb and not cross:
        in_dir2 = analysis_tree.deltas_lyb_dir / 'Delta'
        text += f'--in-dir2 {in_dir2} '

    if cross:
        text += f'--drq {qso_cat} --mode desi_mocks --z-evol-obj 1.44 --rp-min -{rp_max} '
    else:
        text += f'--rp-min {rp_min} '

    text += f'--rp-max {rp_max} ' if not dmat else f'--rp-max {dmat_rp_max} '
    text += f'--rt-max {rt_max} --nt {num_bins_rt} '
    if cross:
        text += f'--np {2*num_bins_rp} ' if not dmat else f'--np {2*dmat_num_bins_rp} '
    else:
        text += f'--np {num_bins_rp} ' if not dmat else f'--np {dmat_num_bins_rp} '

    if zmin != z_min_default:
        text += f'--z-min-pairs {zmin} '
    if zmax != z_max_default:
        text += f'--z-max-pairs {zmax} '

    if cross and dmat:
        text += f'--fid-Om {fid_Om} --nproc {nproc//2} '
    else:
        text += f'--fid-Om {fid_Om} --nproc {nproc} '
    text += f'--fid-Or {fid_Or} --nside {nside} '

    if metal_dmat:
        text += r'--abs-igm SiII\(1260\) SiIII\(1207\) SiII\(1193\) SiII\(1190\) '

    if config.getboolean('no_project'):
        text += '--no-project '

    if dmat or metal_dmat:
        text += f'--rej {dmat_rejection} '
        if coeff_binning is not None:
            text += f'--coef-binning-model {coeff_binning} '

    if cross and config.getboolean('no_remove_mean_lambda_obs'):
        text += '--no-remove-mean-lambda-obs '

    if cross and shuffled:
        text += f'--shuffle-distrib-obj-seed {analysis_tree.mock_seed} '

    if rebin_factor is not None:
        text += f'--rebin-factor {rebin_factor} '
        
    if zerr_cut_deg is not None:
        text += f'--zerr-cut-deg {zerr_cut_deg} '
        
    if zerr_cut_kms is not None:
        text += f'--zerr-cut-kms {zerr_cut_kms} '

    if rmu_binning:
        text += '--rmu-binning '

    text += '\n\n'

    script_path = analysis_tree.scripts_dir / f'{name}_{zmin}_{zmax}.sh'
    if shuffled:
        script_path = analysis_tree.scripts_dir / f'{name}_{zmin}_{zmax}_shuffled.sh'

    submit_utils.write_script(script_path, text)

    job_id = submit_utils.run_job(
        script_path, dependency_ids=delta_job_ids, no_submit=job.getboolean('no_submit'))

    return output_path, job_id
