QQ_DEFAULTS = [
    '--zbest',
    '--bbflux',
    '--zmin 1.7',
    '--save-continuum',
]


QQ_RUN_CODES = {
    '1': '--dla file',
    '2': '--metals LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)',
    '3': '--metals-from-file all',
    '4': '--balprob 0.16'
}


QQ_DEFAULT_METAL_STRENGTHS = {
    'lyacolore': '0.1901 0.0697 0.0335 0.0187 1.3e-03 3.5e-03 0.7e-03 1.4e-03',
    'saclay': '0.1901 0.0697 0.0335 0.0187 5.7e-04 1.6e-03 5.3e-04 6.8e-04'
}


QQ_RUN_ARGS = {
    'desi-test': {
        'exptime': 4000,
        'downsampling': 0.4,
        'sigma_kms_fog': 0.0,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
    },
    'desi-4.0-4': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': ''
    },
    'desi-4.5-4': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': '',
        'sigma_kms_fog': '0'
    },
    'desi-4.12-4': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': '',
        'dla': 'file',
        'metals': 'LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)'
    },
    'desi-4.13-4': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': '',
        'dla': 'file',
        'metals-from-file': 'all'
    },
    'desi-4.124-4-prod': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'dla': 'file',
        'balprob': 0.16,
        'metals': 'LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)',
        'metal-strengths': '0.1901 0.0697 0.0335 0.0187 1.3e-03 3.5e-03 0.7e-03 1.4e-03'
    },
    'desi-4.124-4-prod-saclay': {
        # 'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        # 'desi-footprint': '',
        'zmin': '1.7',
        'dla': 'file',
        'balprob': 0.16,
        'metals': 'LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)',
        'metal-strengths': '0.1901 0.0697 0.0335 0.0187 5.7e-04 1.6e-03 5.3e-04 6.8e-04'
    },
    'desi-4.124-4-metal_tuning_v2': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'dla': 'file',
        'balprob': 0.16,
        'metals': 'LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)',
        # 'metal-strengths': '0.1901 0.0697 0.0335 0.0187 1.3857e-03 6.6112e-03 8.6849e-04 1.6696e-03'
        # 'metal-strengths': '0.1901 0.0697 0.0335 0.0187 1.3e-03 3.5e-03 0.7e-03 1.4e-03'
        # 'metal-strengths': '0.1901 0.0697 0.0335 0.0187 6.5547e-04 2.6013e-03 6.4240e-04 8.5913e-04'
        'metal-strengths': '0.1901 0.0697 0.0335 0.0187 5.7e-04 1.6e-03 5.3e-04 6.8e-04'
    },
    'desi-4.134-4': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': '',
        'dla': 'file',
        'balprob': 0.16,
        'metals-from-file': 'all'
    },
    'desi-4.127-4': {
        'dn_dzdm': '',
        'exptime': 4000,
        'zbest': '',
        'bbflux': '',
        'save-continuum': '',
        'desi-footprint': '',
        'zmin': '1.7',
        'add-LYB': '',
        'dla': 'file',
        'metals': 'LYB LY3 LY4 LY5 SiII(1260) SiIII(1207) SiII(1193) SiII(1190)',
        'gamma_kms_zfit': '400'
    },
}
