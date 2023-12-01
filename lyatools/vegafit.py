from pathlib import Path
from vega import BuildConfig, FitResults, run_vega


def get_correlations_dict(corr_config, corr_path):
    correlations = {'lyaxlya': {}, 'lyaxqso': {}, 'lyaxlyb': {}, 'lybxqso': {}}

    dist_path = Path(corr_config['dist_path'])
    if not dist_path.exists():
        raise ValueError(f'Distortion path does not exist: {dist_path}')

    rmin = corr_config.getfloat('rmin', 10.)
    rmax = corr_config.getfloat('rmax', 180.)
    fast_metals = corr_config.getboolean('fast_metals', True)

    correlations['lyaxlya']['corr_path'] = f"{corr_path / 'cf_lya_lya_0_10-exp.fits.gz'}"
    correlations['lyaxlya']['distortion-file'] = f"{dist_path / 'dmat_lya_lya_0_10.fits.gz'}"
    correlations['lyaxlya']['metal_path'] = f"{dist_path / 'metal_dmat_lya_lya_0_10.fits.gz'}"
    correlations['lyaxlya']['r-min'] = rmin
    correlations['lyaxlya']['r-max'] = rmax
    correlations['lyaxlya']['fast_metals'] = f'{fast_metals}'

    correlations['lyaxlyb']['corr_path'] = f"{corr_path / 'cf_lya_lyb_0_10-exp.fits.gz'}"
    correlations['lyaxlyb']['distortion-file'] = f"{dist_path / 'dmat_lya_lyb_0_10.fits.gz'}"
    correlations['lyaxlyb']['metal_path'] = f"{dist_path / 'metal_dmat_lya_lyb_0_10.fits.gz'}"
    correlations['lyaxlyb']['r-min'] = rmin
    correlations['lyaxlyb']['r-max'] = rmax
    correlations['lyaxlyb']['fast_metals'] = f'{fast_metals}'

    correlations['lyaxqso']['corr_path'] = f"{corr_path / 'xcf_lya_qso_0_10-exp.fits.gz'}"
    correlations['lyaxqso']['distortion-file'] = f"{dist_path / 'xdmat_lya_qso_0_10.fits.gz'}"
    correlations['lyaxqso']['metal_path'] = f"{dist_path / 'metal_xdmat_lya_qso_0_10.fits.gz'}"
    correlations['lyaxqso']['r-min'] = rmin
    correlations['lyaxqso']['r-max'] = rmax
    correlations['lyaxqso']['fast_metals'] = f'{fast_metals}'

    correlations['lybxqso']['corr_path'] = f"{corr_path / 'xcf_lyb_qso_0_10-exp.fits.gz'}"
    correlations['lybxqso']['distortion-file'] = f"{dist_path / 'xdmat_lyb_qso_0_10.fits.gz'}"
    correlations['lybxqso']['metal_path'] = f"{dist_path / 'metal_xdmat_lyb_qso_0_10.fits.gz'}"
    correlations['lybxqso']['r-min'] = rmin
    correlations['lybxqso']['r-max'] = rmax
    correlations['lybxqso']['fast_metals'] = f'{fast_metals}'

    return correlations


def get_builder(builder_config=None):
    options = {
        'scale_params': 'ap_at', 'template': 'PlanckDR12/PlanckDR12.fits',
        'full_shape': False, 'smooth_scaling': False,
        'small_scale_nl': False, 'bao_broadening': False, 'use_metal_autos': True,
        'fullshape_smoothing': 'gauss', 'fullshape_smoothing_metals': True,
        'velocity_dispersion': 'gauss', 'hcd_model': 'Rogers2018',
        'metals': ['SiII(1260)', 'SiIII(1207)', 'SiII(1193)', 'SiII(1190)']
    }

    if builder_config is None:
        return BuildConfig(options, overwrite=True)

    for key in builder_config:
        if builder_config[key] == 'None' and key in options:
            del options[key]
        else:
            if key == 'metals':
                options[key] = builder_config[key].split(' ')
            if key in options:
                options[key] = builder_config[key]

    return BuildConfig(options, overwrite=True)


def get_fit_info(fit_info_config=None):
    fit_info = {
        'fitter': True, 'sampler': True, 'zeff': None, 'zeff_rmin': -300., 'zeff_rmax': 300.,
        'bias_beta_config': {'LYA': 'bias_beta', 'QSO': 'bias_bias_eta'},
        'Polychord': {'num_live': '192', 'boost_posterior': '0'},
        'priors': {'beta_hcd': 'gaussian 0.5 0.09'}
    }

    if fit_info_config is None:
        return fit_info

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

    return fit_info


def build_config(config, corr_path, out_path):
    correlations = get_correlations_dict(config['correlations'], corr_path)
    config_builder = get_builder(config['builder'])
    fit_info = get_fit_info(config['fit_info'])

    fit_type = config['fit_info']['fit_type']
    name_extension = config['fit_info']['name_extension']

    fit_info['sample_params'] = config['fit_info']['sample_params'].split(' ')

    parameters = {}
    for key in config['parameters']:
        parameters[key] = config['parameters'][key]

    vega_res_path = config['fit_info'].get('match_params', None)
    if vega_res_path is not None:
        res = FitResults(vega_res_path)
        parameters = parameters | res.params

    main_path = config_builder.build(
        correlations, fit_type, fit_info, out_path,
        parameters=parameters, name_extension=name_extension
    )

    return main_path


def run_vega_fitter(config, corr_path, out_path, run_flag=True):
    main_path = build_config(config, corr_path, out_path)

    if run_flag:
        run_vega(main_path)
