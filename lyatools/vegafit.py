from pathlib import Path
from vega import BuildConfig, FitResults

from . import submit_utils, dir_handlers


def make_vega_config(analysis_tree, qso_cat, config, job, export_job_id=None, run_local=False):
    correlations = get_correlations_dict(
        config['vega.correlations'], analysis_tree.corr_dir, qso_cat)
    config_builder = get_builder(config['vega.builder'])
    fit_info = get_fit_info(config['vega.fit_info'])

    fit_type = config['vega.fit_info'].get('fit_type')
    name_extension = config['vega.fit_info'].get('name_extension')

    use_full_cov = config['vega.fit_info'].getboolean('use_full_cov', True)
    if use_full_cov and 'global_cov_file' not in fit_info:
        cov_name = config['vega.fit_info'].get('cov_name', 'full_cov_smooth.fits')
        fit_info['global_cov_file'] = str(analysis_tree.corr_dir / cov_name)

    fit_info['sample_params'] = config['vega.fit_info']['sample_params'].split(' ')

    parameters = {}
    for key in config['vega.parameters']:
        parameters[key] = config['vega.parameters'][key]

    vega_res_path = config['vega.fit_info'].get('match_params', None)
    if vega_res_path is not None:
        res = FitResults(vega_res_path)
        parameters = parameters | res.params

    main_path = config_builder.build(
        correlations, fit_type, fit_info, analysis_tree.fits_dir,
        parameters=parameters, name_extension=name_extension
    )

    vega_command = f'run_vega.py {main_path}'
    if not run_local:
        return export_job_id, vega_command

    # Make the header
    time = config['vega.fit_info'].getfloat('slurm_hours', 1.0)
    header = submit_utils.make_header(
        job.get('nersc_machine'), time=time,
        omp_threads=64, job_name=f'vega_{analysis_tree.full_mock_seed}',
        err_file=analysis_tree.logs_dir/f'vega-{analysis_tree.full_mock_seed}-%j.err',
        out_file=analysis_tree.logs_dir/f'vega-{analysis_tree.full_mock_seed}-%j.out'
    )

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'
    text += vega_command + '\n'

    # Write the script.
    script_path = analysis_tree.scripts_dir / 'vegafit.sh'
    submit_utils.write_script(script_path, text)

    job_id = export_job_id
    job_id = submit_utils.run_job(
        script_path, dependency_ids=export_job_id, no_submit=job.getboolean('no_submit'))

    return job_id, vega_command


def run_vega_mpi(vega_commands, analysis_tree, config, job, export_job_ids=None):
    # Make the header
    time = config['vega.fit_info'].getfloat('slurm_hours', 1.0)
    num_cores_per_node = config['vega.fit_info'].getint('num_cores_per_node')
    num_nodes = config['vega.fit_info'].getint('num_nodes', 1)

    if num_cores_per_node is None:
        num_cores_per_node = min(len(vega_commands), 25)
    if num_nodes is None:
        num_nodes = len(vega_commands) // num_cores_per_node

    if num_nodes * num_cores_per_node > len(vega_commands):
        raise ValueError(
            f'Number of cores #{num_nodes}x{num_cores_per_node} exceeds '
            f'number of commands #{len(vega_commands)}.'
        )

    header = submit_utils.make_header(
        job.get('nersc_machine'), time=time, nodes=num_nodes,
        omp_threads=2, job_name='fit_mocks',
        err_file=analysis_tree.logs_dir/'mpi_vegafit-%j.err',
        out_file=analysis_tree.logs_dir/'mpi_vegafit-%j.out'
    )

    # Create the script
    text = header
    env_command = job.get('env_command')
    text += f'{env_command}\n\n'

    logs = analysis_tree.logs_dir / 'vega_logs'
    dir_handlers.check_dir(logs)

    text_commands = '"' + '" "'.join(vega_commands) + '"'
    text += f'srun --ntasks-per-node={num_cores_per_node} lyatools-mpi-export -i {text_commands} '
    text += f'-l {logs}\n'

    # Write the script.
    script_path = analysis_tree.scripts_dir / 'mpi_vegafit.sh'
    submit_utils.write_script(script_path, text)

    _ = submit_utils.run_job(
        script_path, dependency_ids=export_job_ids, no_submit=job.getboolean('no_submit'))


def get_correlations_dict(config, corr_dir, qso_cat):
    correlations = {'lyaxlya': {}, 'lyaxqso': {}, 'lyaxlyb': {}, 'lybxqso': {}}

    dist_path = Path(config['distortion_path'])
    if not dist_path.exists():
        raise ValueError(f'Distortion path does not exist: {dist_path}')

    rmin = config.getfloat('rmin')
    rmax = config.getfloat('rmax')
    rmin_auto = config.getfloat('rmin-auto', rmin)
    rmin_cross = config.getfloat('rmin-cross', rmin)
    fast_metals = config.getboolean('fast-metals')

    correlations['lyaxlya']['corr_path'] = f"{corr_dir / 'cf_lya_lya_0_10-exp.fits.gz'}"
    correlations['lyaxlya']['distortion-file'] = f"{dist_path / 'dmat_lya_lya_0_10.fits'}"
    # correlations['lyaxlya']['metal_path'] = f"{dist_path / 'metal_dmat_lya_lya_0_10.fits.gz'}"
    correlations['lyaxlya']['weights-tracer1'] = \
        f"{corr_dir.parent / 'deltas_lya/Log/delta_attributes.fits.gz'}"
    correlations['lyaxlya']['weights-tracer2'] = \
        f"{corr_dir.parent / 'deltas_lya/Log/delta_attributes.fits.gz'}"
    correlations['lyaxlya']['r-min'] = rmin_auto
    correlations['lyaxlya']['r-max'] = rmax
    correlations['lyaxlya']['fast_metals'] = f'{fast_metals}'

    correlations['lyaxlyb']['corr_path'] = f"{corr_dir / 'cf_lya_lyb_0_10-exp.fits.gz'}"
    correlations['lyaxlyb']['distortion-file'] = f"{dist_path / 'dmat_lya_lyb_0_10.fits'}"
    # correlations['lyaxlyb']['metal_path'] = f"{dist_path / 'metal_dmat_lya_lyb_0_10.fits.gz'}"
    correlations['lyaxlyb']['weights-tracer1'] = \
        f"{corr_dir.parent / 'deltas_lya/Log/delta_attributes.fits.gz'}"
    correlations['lyaxlyb']['weights-tracer2'] = \
        f"{corr_dir.parent / 'deltas_lyb/Log/delta_attributes.fits.gz'}"
    correlations['lyaxlyb']['r-min'] = rmin_auto
    correlations['lyaxlyb']['r-max'] = rmax
    correlations['lyaxlyb']['fast_metals'] = f'{fast_metals}'

    correlations['lyaxqso']['corr_path'] = f"{corr_dir / 'xcf_lya_qso_0_10-exp.fits.gz'}"
    correlations['lyaxqso']['distortion-file'] = f"{dist_path / 'xdmat_lya_qso_0_10.fits'}"
    # correlations['lyaxqso']['metal_path'] = f"{dist_path / 'metal_xdmat_lya_qso_0_10.fits.gz'}"
    correlations['lyaxqso']['weights-tracer1'] = \
        f"{corr_dir.parent / 'deltas_lya/Log/delta_attributes.fits.gz'}"
    correlations['lyaxqso']['weights-tracer2'] = f"{qso_cat}"
    correlations['lyaxqso']['r-min'] = rmin_cross
    correlations['lyaxqso']['r-max'] = rmax
    correlations['lyaxqso']['fast_metals'] = f'{fast_metals}'

    correlations['lybxqso']['corr_path'] = f"{corr_dir / 'xcf_lyb_qso_0_10-exp.fits.gz'}"
    correlations['lybxqso']['distortion-file'] = f"{dist_path / 'xdmat_lyb_qso_0_10.fits'}"
    # correlations['lybxqso']['metal_path'] = f"{dist_path / 'metal_xdmat_lyb_qso_0_10.fits.gz'}"
    correlations['lybxqso']['weights-tracer1'] = \
        f"{corr_dir.parent / 'deltas_lyb/Log/delta_attributes.fits.gz'}"
    correlations['lybxqso']['weights-tracer2'] = f"{qso_cat}"
    correlations['lybxqso']['r-min'] = rmin_cross
    correlations['lybxqso']['r-max'] = rmax
    correlations['lybxqso']['fast_metals'] = f'{fast_metals}'

    return correlations


def get_builder(builder_config):
    options = {
        'scale_params': 'ap_at', 'template': 'PlanckDR12/PlanckDR12.fits',
        'full_shape': False, 'smooth_scaling': False,
        'small_scale_nl': False, 'bao_broadening': False, 'use_metal_autos': True,
        'fullshape_smoothing': 'gauss', 'fullshape_smoothing_metals': True,
        'velocity_dispersion': 'gauss', 'hcd_model': 'Rogers2018',
        'metals': ['SiII(1260)', 'SiIII(1207)', 'SiII(1193)', 'SiII(1190)'],
        'new_metals': True,
    }
    for key in builder_config:
        if builder_config[key] == 'None' and key in options:
            del options[key]
        else:
            if key == 'metals':
                options[key] = builder_config[key].split(' ')
            else:
                # TODO fix booleans
                options[key] = builder_config[key]

    return BuildConfig(options, overwrite=True)


def get_fit_info(fit_info_config):
    fit_info = {
        'run_sampler': True, 'zeff': None, 'zeff_rmin': -300., 'zeff_rmax': 300.,
        'bias_beta_config': {'LYA': 'bias_beta', 'QSO': 'bias_bias_eta'},
        'Polychord': {'num_live': '192', 'boost_posterior': '0'},
        'priors': {'beta_hcd': 'gaussian 0.5 0.09'},
        'use_template_growth_rate': 'False',
    }

    for key in fit_info_config:
        if 'bias_beta_config' in key:
            _, key2 = key.split('.')
            fit_info['bias_beta_config'][key2] = fit_info_config[key]

        elif 'Polychord' in key:
            _, key2 = key.split('.')
            fit_info['Polychord'][key2] = fit_info_config[key]
        elif 'priors' in key:
            if key == 'priors.None':
                fit_info['priors'] = {}
            else:
                _, key2 = key.split('.')
                fit_info['priors'][key2] = fit_info_config[key]

        elif key in fit_info:
            fit_info[key] = fit_info_config[key]

        if key == 'global_cov_file':
            fit_info['global_cov_file'] = fit_info_config[key]

    return fit_info
