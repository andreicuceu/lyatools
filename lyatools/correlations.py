from subprocess import run

from . import submit_utils

JOB_CONFIGS = {'cf_lya_lya': 2.0, 'dmat_lya_lya': 2.0,
               'cf_lya_lyb': 1.2, 'dmat_lya_lyb': 1.2,
               'xcf_lya_qso': 0.5, 'xdmat_lya_qso': 0.5,
               'xcf_lyb_qso': 0.25, 'xdmat_lyb_qso': 0.25}


def make_correlation_runs(args, analysis_dir, corr_types, catalogue):
    submit_utils.set_umask()

    if 'lya_lya' in corr_types:
        if not args.no_compute_corr:
            run_correlation(args, analysis_dir, run_name='cf_lya_lya')
        if args.compute_dmat:
            run_correlation(args, analysis_dir, dmat=True, run_name='dmat_lya_lya')

    if 'lya_lyb' in corr_types:
        if args.deltas_dir_lyb is None:
            raise ValueError('Asked for lya_lyb correlation, but did not give path to lyb deltas')

        if not args.no_compute_corr:
            run_correlation(args, analysis_dir, lyb=True, run_name='cf_lya_lyb')
        if args.compute_dmat:
            run_correlation(args, analysis_dir, lyb=True, dmat=True, run_name='dmat_lya_lyb')

    if 'lya_qso' in corr_types:
        if args.catalogue is None:
            raise ValueError('Asked for lya_qso correlation, but did not give path to qso catalog')

        if not args.no_compute_corr:
            run_correlation(args, analysis_dir, catalogue, cross=True, run_name='xcf_lya_qso')
        if args.compute_dmat:
            run_correlation(args, analysis_dir, catalogue, cross=True, dmat=True,
                            run_name='xdmat_lya_qso')

    if 'lyb_qso' in corr_types:
        if args.deltas_dir_lyb is None:
            raise ValueError('Asked for lyb_qso correlation, but did not give path to lyb deltas')
        if args.catalogue is None:
            raise ValueError('Asked for lya_qso correlation, but did not give path to qso catalog')

        if not args.no_compute_corr:
            run_correlation(args, analysis_dir, catalogue, cross=True, lyb=True,
                            run_name='xcf_lyb_qso')
        if args.compute_dmat:
            run_correlation(args, analysis_dir, catalogue, cross=True, lyb=True, dmat=True,
                            run_name='xdmat_lyb_qso')


def run_correlation(args, analysis_dir, catalogue=None, cross=False, lyb=False, dmat=False,
                    run_name='cf_lya_lya'):
    time_hours = args.slurm_hours
    if time_hours is None:
        time_hours = JOB_CONFIGS[run_name]

    time = submit_utils.convert_job_time(time_hours)

    # Make the header
    header = submit_utils.make_header(args.nersc_machine, args.queue, time=time,
                                      omp_threads=args.nproc, job_name=run_name,
                                      err_file=analysis_dir.logs_dir/f'{run_name}-%j.err',
                                      out_file=analysis_dir.logs_dir/f'{run_name}-%j.out')

    zmin, zmax = args.z_limits
    script_type = run_name.split('_')[0]
    if args.name_string is None:
        output_path = analysis_dir.corr_dir / f'{run_name}_{zmin}_{zmax}.fits.gz'
    else:
        output_path = analysis_dir.corr_dir / f'{run_name}_{zmin}_{zmax}_{args.name_string}.fits.gz'

    # Create the script
    text = header
    text += f'{args.env_command}\n\n'
    text += f'srun -n 1 -c {args.nproc} picca_{script_type}.py '
    text += f'--out {output_path} '

    if cross and lyb:
        text += f'--in-dir {analysis_dir.deltas_lyb_dir} '
    else:
        text += f'--in-dir {analysis_dir.deltas_lya_dir} '

    if lyb and not cross:
        text += f'--in-dir2 {analysis_dir.deltas_lyb_dir} '

    if cross:
        text += f'--drq {catalogue} --mode desi_mocks --z-evol-obj 1.44 --rp-min -200 '
    else:
        text += f'--rp-min {args.rp_min} '

    text += f'--rp-max {args.rp_max} --rt-max {args.rt_max} --nt {args.num_bins_rt} '
    if cross:
        text += f'--np {2*args.num_bins_rp} '
    else:
        text += f'--np {args.num_bins_rp} '

    text += f'--z-cut-min {zmin} --z-cut-max {zmax} --fid-Om {args.fid_Om} --nproc {args.nproc} '
    text += '--fid-Or 7.97505418919554e-05 '

    if args.no_project:
        text += '--no-project '

    if dmat:
        text += f'--rej {args.dmat_rejection} '

    if cross and args.no_remove_mean_lambda_obs:
        text += '--no-remove-mean-lambda-obs '

    if args.rebin_factor is not None:
        text += f'--rebin-factor {args.rebin_factor} '

    text += '\n\n'

    slurm_script_path = analysis_dir.scripts_dir / f'{run_name}_{zmin}_{zmax}.sh'

    # Write the script.
    with open(slurm_script_path, 'w') as f:
        f.write(text)
    submit_utils.make_file_executable(slurm_script_path)

    # Run the script
    if not args.no_submit and not output_path.is_file():
        run(['sbatch', slurm_script_path])
