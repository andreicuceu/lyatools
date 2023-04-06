import numpy as np
from pathlib import Path
from subprocess import run

from . import submit_utils, dir_handlers
from lyatools.picca_utils import desi_from_ztarget_to_drq
from lyatools.quickquasars import run_qq
from lyatools.delta_extraction import run_true_continuum, run_continuum_fitting
from lyatools.correlations import make_correlation_runs
from lyatools.stack import stack_export_correlations


def get_seed_list(qq_seeds):
    # Get list of seeds
    run_seeds = []
    for seed in qq_seeds:
        seed_range = seed.split('-')

        if len(seed_range) == 1:
            run_seeds.append(int(seed_range[0]))
        elif len(seed_range) == 2:
            run_seeds += list(np.arange(int(seed_range[0]), int(seed_range[1])))
        else:
            raise ValueError(f'Unknown seed type {seed}. Must be int or range (e.g. 0-5)')

    run_seeds.sort()
    return run_seeds


def multi_run_qq(input_dir_all, output_dir_all, qq_seeds, qq_run_type, test_run, no_submit, *args):
    """Create and submit QQ runs for multiple mock realizations

    Parameters
    ----------
    input_dir_all : str
        Raw directory with all v9.0 lyacolore runs
    output_dir_all : str
        Directory that contains all of the qq realisations (dir with v9.0.x).
    qq_seeds : str
        QQ seeds to run. Either a single string (e.g. '0') or a range (e.g. '0-5').
    qq_run_type : str
        Run type. Must be a key in the QQ_RUN_ARGS dict.
    test_run : bool
        Test run flag
    no_submit : bool
        Submit flag
    args : list
        List with args passed to create_qq_script
    """
    run_seeds = get_seed_list(qq_seeds)

    # Submit QQ run for each seed
    for seed in run_seeds:
        input_dir = Path(input_dir_all) / f'v9.0.{seed}'
        output_dir = Path(output_dir_all) / f'v9.0.{seed}'
        print(f'Submitting QQ run for mock v9.0.{seed}')

        run_qq(qq_run_type, test_run, no_submit, input_dir, output_dir, *args)


def multi_run_delta_extraction(args):
    if args.test_run:
        print('test run enabled, overriding "--qq_run_type" to set to "desi-test"')
        args.qq_run_type = 'desi-test'
        print('test run enabled, overriding "--slurm-queue" to set to "debug"')
        args.slurm_queue = 'debug'
        submit_utils.print_spacer_line()

    run_seeds = get_seed_list(args.qq_seeds)

    for seed in run_seeds:
        qq_dir = Path(args.input_dir) / f'v9.0.{seed}' / f'{args.qq_run_type}'
        print(f'Making catalogues for quickquasars run in {qq_dir}')

        # Make the zcat if it does not exist already
        zcat_file = qq_dir / 'zcat.fits'
        if not zcat_file.is_file():
            print('Making DESI zcat')
            command = f'make_zcat.py --qq-dir {qq_dir} --out zcat.fits'
            process = run(command, shell=True)
            if process.returncode != 0:
                raise ValueError(f'Running command "{command}" returned non-zero exitcode '
                                 f'with error {process.stderr}')
            submit_utils.print_spacer_line()

        # Make the drq if it does not exist already
        drq_file = qq_dir / 'drq_qso.fits'
        if not drq_file.is_file():
            desi_from_ztarget_to_drq(zcat_file, drq_file)
            submit_utils.print_spacer_line()

        main_path = Path(args.output_dir) / f'v9.0.{seed}'
        if args.run_true_continuum:
            run_true_continuum(args, qq_dir, main_path, zcat_file)

        if not args.no_run_continuum_fitting:
            run_continuum_fitting(args, qq_dir, main_path, zcat_file)


def multi_run_correlations(args):
    run_seeds = get_seed_list(args.qq_seeds)

    for seed in run_seeds:
        main_path = Path(args.input_dir) / f'v9.0.{seed}'
        qq_dir = Path(args.qq_dir) / f'v9.0.{seed}' / f'{args.qq_run_type}'
        zcat_file = qq_dir / 'zcat.fits'

        if args.run_type is None:
            args.run_type = args.qq_run_type

        corr_types = []
        if args.run_auto:
            corr_types += ['lya_lya']
            if args.run_lyb_region:
                corr_types += ['lya_lyb']
        if args.run_cross:
            corr_types += ['lya_qso']
            if args.run_lyb_region:
                corr_types += ['lyb_qso']

        if len(corr_types) < 1:
            raise ValueError('Did not find anything to run. Add "run_auto" and/or "run_cross".')

        if args.run_true_continuum:
            name = 'true_cont'
            if args.analysis_name is not None:
                name += f'_{args.analysis_name}'
            analysis_dir = dir_handlers.AnalysisDir(main_path, args.run_type, name)

            make_correlation_runs(args, analysis_dir, corr_types, zcat_file)

        if not args.no_run_continuum_fitting:
            name = 'baseline'
            if args.analysis_name is not None:
                name = args.analysis_name
            analysis_dir = dir_handlers.AnalysisDir(main_path, args.run_type, name)

            make_correlation_runs(args, analysis_dir, corr_types, zcat_file)


def multi_export(args):
    run_seeds = get_seed_list(args.qq_seeds)

    types = {'cf_lya_lya_0_10': 'dmat_lya_lya_0_10', 'cf_lya_lyb_0_10': 'dmat_lya_lyb_0_10',
             'xcf_lya_qso_0_10': 'xdmat_lya_qso_0_10', 'xcf_lyb_qso_0_10': 'xdmat_lyb_qso_0_10'}

    if args.name_string is not None:
        name_addon = f'_{args.name_string}'
        types = {key + name_addon: val + name_addon for key, val in types.items()}

    # Go through each seed and export all correlations we find
    corr_dict = {key: [] for key in types}
    for seed in run_seeds:
        main_path = Path(args.input_dir) / f'v9.0.{seed}' / args.run_type
        corr_path = main_path / args.analysis_name / 'correlations'

        for cf, dmat in types.items():
            file = corr_path / f'{cf}.fits.gz'
            exp_file = corr_path / f'{cf}-exp.fits.gz'

            if file.is_file():
                corr_dict[cf] += [file]

            if file.is_file() and not exp_file.is_file():
                # Do the exporting
                command = f'picca_export.py --data {file} --out {exp_file} '

                if args.add_dmat:
                    dmat_file = Path(args.dmat_path) / f'{dmat}.fits.gz'
                    if not dmat_file.is_file():
                        raise ValueError(f'Asked for dmat, but dmat {dmat_file} could not be found')
                    command += f'--dmat {dmat_file} '

                run(command, shell=True)
                submit_utils.print_spacer_line()

    # Stack correlations from different seeds
    if args.stack_correlations:
        for cf_name, cf_list in corr_dict.items():
            if len(cf_list) < 1:
                continue

            str_list = [str(cf) for cf in cf_list]
            in_files = ' '.join(str_list)

            global_run_path = Path(args.stack_out_dir) / args.run_type
            dir_handlers.check_dir(global_run_path)

            global_analysis_path = global_run_path / args.analysis_name
            dir_handlers.check_dir(global_analysis_path)

            global_corr_path = global_analysis_path / 'correlations'
            dir_handlers.check_dir(global_corr_path)

            exp_out_file = global_corr_path / f'{cf_name}-exp.fits.gz'

            dmat_file = None
            if args.add_dmat:
                dmat = types[cf_name]
                dmat_file = Path(args.dmat_path) / f'{dmat}.fits.gz'
                if not dmat_file.is_file():
                    raise ValueError(f'Asked for dmat, but dmat {dmat_file} could not be found')

            stack_export_correlations(in_files, exp_out_file, dmat_file)

    print('Done')
