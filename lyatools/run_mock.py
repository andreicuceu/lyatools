import configparser
from pathlib import Path

from . import submit_utils, dir_handlers
from lyatools.raw_deltas import make_raw_deltas
from lyatools.quickquasars import run_qq
from lyatools.delta_extraction import make_delta_runs
from lyatools.qsonic import make_qsonic_runs
from lyatools.correlations import make_correlation_runs
from lyatools.export import make_export_runs, stack_correlations


class RunMocks:
    def __init__(self, config_path):
        # Read default config and overwrite with input config
        self.config = configparser.ConfigParser()
        self.config.read(submit_utils.find_path('defaults/desi_y5.ini'))
        self.config.read(submit_utils.find_path(config_path))

        # Save sections we'll need later
        self.job = self.config['job_info']
        # self.control = self.config['control']
        self.qq = self.config['quickquasars']
        self.deltas = self.config['delta_extraction']
        self.qsonic = self.config['qsonic']
        self.corr = self.config['picca_corr']
        self.export = self.config['picca_export']

        # Get mock setup
        self.input_dir = self.config['mock_setup'].get('input_dir')
        self.qq_dir = self.config['mock_setup'].get('qq_dir')
        self.analysis_dir = self.config['mock_setup'].get('analysis_dir')

        self.mock_code = self.config['mock_setup'].get('mock_code')
        self.mock_version = self.config['mock_setup'].get('mock_version')
        self.qq_seeds = self.config['mock_setup'].get('qq_seeds')
        self.qq_run_type = self.config['mock_setup'].get('qq_run_type')
        self.run_type = self.config['mock_setup'].get('run_type')
        self.analysis_name = self.config['mock_setup'].get('analysis_name')

        if self.job.getboolean('test_run'):
            self.qq_run_type = 'desi-test'

        if self.run_type is None:
            self.run_type = self.qq_run_type

        # Get control flags
        self.run_qq_flag = self.config['control'].getboolean('run_qq')
        self.run_deltas_flag = self.config['control'].getboolean('run_deltas')
        self.run_qsonic_flag = self.config['control'].getboolean('run_qsonic')
        self.run_corr_flag = self.config['control'].getboolean('run_corr')
        self.run_export_flag = self.config['control'].getboolean('run_export')

        self.run_raw_flag = self.config['control'].getboolean('run_raw')
        self.run_true_cont_flag = self.config['control'].getboolean('run_true_continuum')
        self.no_run_cont_fit_flag = self.config['control'].getboolean('no_run_continuum_fitted')

        self.dla_flag = None

    def run_mocks(self):
        print(f'Running Lyman-alpha forest mocks with seeds {self.qq_seeds}.')
        submit_utils.print_spacer_line()

        run_seeds = submit_utils.get_seed_list(self.qq_seeds)

        raw_corr_dict = {}
        true_corr_dict = {}
        corr_dict = {}

        raw_exp_job_ids = []
        true_exp_job_ids = []
        exp_job_ids = []

        for seed in run_seeds:
            # Run QQ
            zcat_job_id = None
            if self.run_qq_flag:
                qq_job_id = self.run_qq(seed)
                submit_utils.print_spacer_line()

                zcat_job_id = self.run_zcat(seed, qq_job_id)
                submit_utils.print_spacer_line()

                if self.dla_flag:
                    dlacat_job_id = self.run_dla_cat(seed, qq_job_id)
                    submit_utils.print_spacer_line()
                    zcat_job_id = [zcat_job_id, dlacat_job_id]

            analysis_struct, true_analysis_struct, \
                raw_analysis_struct = self.get_analysis_dirs(seed)

            # Run raw analysis
            if self.run_raw_flag:
                self.save_config(raw_analysis_struct)
                corr_files, job_id = self.run_analysis(seed, raw_analysis_struct,
                                                       true_continuum=False, raw_analysis=True,
                                                       zcat_job_id=zcat_job_id)
                raw_exp_job_ids += [job_id]
                for key, file in corr_files.items():
                    if key not in raw_corr_dict:
                        raw_corr_dict[key] = []
                    raw_corr_dict[key] += [file]

            # Run true continuum analysis
            if self.run_true_cont_flag:
                self.save_config(true_analysis_struct)
                corr_files, job_id = self.run_analysis(seed, true_analysis_struct,
                                                       true_continuum=True, raw_analysis=False,
                                                       zcat_job_id=zcat_job_id)
                true_exp_job_ids += [job_id]
                for key, file in corr_files.items():
                    if key not in true_corr_dict:
                        true_corr_dict[key] = []
                    true_corr_dict[key] += [file]

            # Run true continuum analysis
            if not self.no_run_cont_fit_flag:
                self.save_config(analysis_struct)
                corr_files, job_id = self.run_analysis(seed, analysis_struct, true_continuum=False,
                                                       raw_analysis=False, zcat_job_id=zcat_job_id)
                exp_job_ids += [job_id]
                for key, file in corr_files.items():
                    if key not in corr_dict:
                        corr_dict[key] = []
                    corr_dict[key] += [file]

            submit_utils.print_spacer_line()

        if self.export.getboolean('stack_correlations'):
            global_struct, true_global_struct, raw_global_struct = self.get_global_struct()

            add_dmat = self.export.getboolean('add_dmat')
            dmat_path = self.export.get('dmat_path')
            name_string = self.corr.get('name_string', None)

            if self.run_raw_flag:
                print('Starting stack and export job for raw deltas.')
                _ = stack_correlations(raw_corr_dict, raw_global_struct, self.job,
                                       add_dmat=add_dmat, dmat_path=dmat_path,
                                       shuffled=self.export.getboolean('subtract_shuffled'),
                                       name_string=name_string, exp_job_ids=raw_exp_job_ids)
                submit_utils.print_spacer_line()

            if self.run_true_cont_flag:
                print('Starting stack and export job for true deltas.')
                _ = stack_correlations(true_corr_dict, true_global_struct, self.job,
                                       add_dmat=add_dmat, dmat_path=dmat_path,
                                       shuffled=self.export.getboolean('subtract_shuffled'),
                                       name_string=name_string, exp_job_ids=true_exp_job_ids)
                submit_utils.print_spacer_line()

            if not self.no_run_cont_fit_flag:
                print('Starting stack and export job for fitted deltas.')
                _ = stack_correlations(corr_dict, global_struct, self.job,
                                       add_dmat=add_dmat, dmat_path=dmat_path, shuffled=False,
                                       name_string=name_string, exp_job_ids=exp_job_ids)
                submit_utils.print_spacer_line()

        submit_utils.print_spacer_line()
        print('Done')
        submit_utils.print_spacer_line()

    def run_analysis(self, seed, analysis_struct, true_continuum=False, raw_analysis=False,
                     zcat_job_id=None):
        # Run deltas
        delta_job_ids = None
        if self.run_deltas_flag:
            if raw_analysis:
                print(f'Starting raw deltas jobs for seed {seed}.')
                delta_job_ids = self.run_raw_deltas(seed, analysis_struct, zcat_job_id=zcat_job_id)
            else:
                print(f'Starting delta extraction jobs for seed {seed}.')
                delta_job_ids = self.run_delta_extraction(seed, analysis_struct,
                                                          true_continuum=true_continuum,
                                                          zcat_job_id=zcat_job_id)
        submit_utils.print_spacer_line()

        if self.run_qsonic_flag:
            print(f'Starting qsonic jobs for seed {seed}.')
            qsonic_job_ids = self.run_qsonic(seed, analysis_struct, true_continuum=true_continuum,
                                             zcat_job_id=zcat_job_id)

            if delta_job_ids is not None:
                delta_job_ids += qsonic_job_ids

        # Run correlations
        corr_paths = None
        corr_job_ids = None
        if self.run_corr_flag:
            print(f'Starting correlation jobs for seed {seed}.')
            corr_paths, corr_job_ids = self.run_correlations(seed, analysis_struct,
                                                             delta_job_ids=delta_job_ids,
                                                             raw_analysis=raw_analysis)
        submit_utils.print_spacer_line()

        # Run export
        corr_files = {}
        job_id = None
        if self.run_export_flag:
            print(f'Starting export jobs for seed {seed}.')
            corr_files, job_id = self.run_export(seed, analysis_struct, corr_paths,
                                                 corr_job_ids=corr_job_ids)
        submit_utils.print_spacer_line()

        return corr_files, job_id

    def run_qq(self, seed):
        input_dir = Path(self.input_dir) / f'{self.mock_version}.{seed}'
        output_dir = Path(self.qq_dir) / f'{self.mock_version}.{seed}'
        print(f'Submitting QQ run for mock {self.mock_version}.{seed}')

        qq_job_id, self.dla_flag = run_qq(self.qq, self.job, self.qq_run_type, seed,
                                          self.mock_code, input_dir, output_dir)

        return qq_job_id

    def run_zcat(self, seed, qq_job_id=None):
        main_path = Path(self.qq_dir) / f'{self.mock_version}.{seed}'
        qq_struct = dir_handlers.QQDir(main_path, self.qq_run_type)
        self.save_config(qq_struct)

        # Make the zcat if it does not exist already
        zcat_file = qq_struct.qq_dir / 'zcat.fits'
        if zcat_file.is_file():
            return

        print('Submitting DESI zcat job')
        header = submit_utils.make_header(self.job.get('nersc_machine'), nodes=1, time=0.1,
                                          omp_threads=128, job_name=f'zcat_{seed}',
                                          err_file=qq_struct.log_dir/'run-%j.err',
                                          out_file=qq_struct.log_dir/'run-%j.out')

        text = header
        text += 'source /global/common/software/desi/desi_environment.sh master\n\n'
        text += f'desi_zcatalog -i {qq_struct.spectra_dir} -o {zcat_file} '
        text += '--minimal --prefix zbest\n'

        script_path = qq_struct.scripts_dir / 'make_zcat.sh'
        submit_utils.write_script(script_path, text)

        zcat_job_id = submit_utils.run_job(script_path, dependency_ids=qq_job_id,
                                           no_submit=self.job.getboolean('no_submit'))

        return zcat_job_id

    def run_dla_cat(self, seed, qq_job_id=None):
        main_path = Path(self.qq_dir) / f'{self.mock_version}.{seed}'
        qq_struct = dir_handlers.QQDir(main_path, self.qq_run_type)

        print('Submitting DLA catalog job')
        header = submit_utils.make_header(self.job.get('nersc_machine'), nodes=1, time=0.2,
                                          omp_threads=128, job_name=f'dlacat_{seed}',
                                          err_file=qq_struct.log_dir/'run-%j.err',
                                          out_file=qq_struct.log_dir/'run-%j.out')

        text = header
        env_command = self.job.get('env_command')
        text += f'{env_command}\n\n'
        text += f'lyatools-make-dla-cat -i {qq_struct.spectra_dir} -o {qq_struct.qq_dir} '

        mask_nhi_cut = self.qq.getfloat('dla_mask_nhi_cut')
        text += f'--mask-nhi-cut {mask_nhi_cut} --nproc {128}'

        script_path = qq_struct.scripts_dir / 'make_dlacat.sh'
        submit_utils.write_script(script_path, text)

        dlacat_job_id = submit_utils.run_job(script_path, dependency_ids=qq_job_id,
                                             no_submit=self.job.getboolean('no_submit'))

        return dlacat_job_id

    def run_raw_deltas(self, seed, analysis_struct, zcat_job_id=None):
        input_dir = Path(self.input_dir) / f'{self.mock_version}.{seed}'
        main_path = Path(self.qq_dir) / f'{self.mock_version}.{seed}'
        qq_struct = dir_handlers.QQDir(main_path, self.qq_run_type)

        zcat_file = self.deltas.get('raw_catalog')
        if zcat_file is None:
            zcat_file = qq_struct.qq_dir / 'zcat.fits'
        else:
            zcat_file = submit_utils.find_path(zcat_file)

        delta_job_ids = make_raw_deltas(input_dir, zcat_file, analysis_struct,
                                        self.job, zcat_job_id=zcat_job_id,
                                        run_lyb_region=self.deltas.getboolean('run_lyb_region'),
                                        delta_lambda=self.deltas.getfloat('delta_lambda'),
                                        max_num_spec=self.deltas.getint('max_num_spec', None),
                                        use_old_weights=self.deltas.getboolean('use_old_weights'))

        return delta_job_ids

    def run_delta_extraction(self, seed, analysis_struct, true_continuum=False,
                             zcat_job_id=None):
        qq_dir = Path(self.qq_dir) / f'{self.mock_version}.{seed}' / f'{self.qq_run_type}'
        zcat_file = qq_dir / 'zcat.fits'

        mask_dla_flag = self.deltas.getboolean('mask_DLAs')
        mask_dla_cat = None
        if mask_dla_flag:
            if self.dla_flag is not None and not self.dla_flag:
                raise ValueError('Asked for DLA masking but there are no DLAs in the qq run')

            mask_nhi_cut = self.qq.getfloat('dla_mask_nhi_cut')
            mask_dla_cat = qq_dir / f'dla_cat_mask_{mask_nhi_cut:.2f}.fits'

        delta_job_ids = make_delta_runs(self.deltas, self.job, qq_dir, zcat_file, analysis_struct,
                                        mask_dla_cat, zcat_job_id, true_continuum=true_continuum)

        return delta_job_ids

    def run_qsonic(self, seed, analysis_struct, true_continuum=False, zcat_job_id=None):
        qq_dir = Path(self.qq_dir) / f'{self.mock_version}.{seed}' / f'{self.qq_run_type}'
        zcat_file = qq_dir / 'zcat.fits'

        mask_dla_flag = self.deltas.getboolean('mask_DLAs')
        mask_dla_cat = None
        if mask_dla_flag:
            if self.dla_flag is not None and not self.dla_flag:
                raise ValueError('Asked for DLA masking but there are no DLAs in the qq run')

            mask_nhi_cut = self.qq.getfloat('dla_mask_nhi_cut')
            mask_dla_cat = qq_dir / f'dla_cat_mask_{mask_nhi_cut:.2f}.fits'

        qsonic_job_ids = make_qsonic_runs(self.qsonic, self.job, qq_dir, zcat_file, analysis_struct,
                                          mask_dla_cat, zcat_job_id, true_continuum=true_continuum)

        return qsonic_job_ids

    def run_correlations(self, seed, analysis_struct, delta_job_ids=None, raw_analysis=False):
        qq_dir = Path(self.qq_dir) / f'{self.mock_version}.{seed}' / f'{self.qq_run_type}'
        zcat_file = qq_dir / 'zcat.fits'
        if raw_analysis and self.deltas.get('raw_catalog') is not None:
            zcat_file = submit_utils.find_path(self.deltas.get('raw_catalog'))

        corr_types = []
        if self.corr.getboolean('run_auto'):
            corr_types += ['lya_lya']

            if self.corr.getboolean('run_lyb_region'):
                corr_types += ['lya_lyb']

        if self.corr.getboolean('run_cross'):
            corr_types += ['lya_qso']

            if self.corr.getboolean('run_lyb_region'):
                corr_types += ['lyb_qso']

        if len(corr_types) < 1:
            raise ValueError('Did not find anything to run. Add "run_auto" and/or "run_cross".')

        corr_paths, corr_job_ids = make_correlation_runs(self.corr, self.job, analysis_struct,
                                                         corr_types, zcat_file, delta_job_ids)

        return corr_paths, corr_job_ids

    def run_export(self, seed, analysis_struct, corr_paths, corr_job_ids=None):
        if corr_paths is None:
            raise ValueError('Export only runs must include correlation runs as well. '
                             'In the [control] section set "run_corr" to True. '
                             'Correlations are *not* recomputed if they already exist.')

        corr_dict, job_id = make_export_runs(seed, analysis_struct, corr_paths, self.job,
                                             add_dmat=self.export.getboolean('add_dmat'),
                                             dmat_path=self.export.get('dmat_path'),
                                             shuffled=self.export.getboolean('subtract_shuffled'),
                                             corr_job_ids=corr_job_ids)

        return corr_dict, job_id

    def get_analysis_dirs(self, seed):
        main_path = Path(self.analysis_dir) / f'{self.mock_version}.{seed}'

        raw_analysis_struct = None
        if self.run_raw_flag:
            name = 'raw'
            if self.analysis_name is not None:
                name += f'_{self.analysis_name}'
            raw_analysis_struct = dir_handlers.AnalysisDir(main_path, self.run_type, name)

        true_analysis_struct = None
        if self.run_true_cont_flag:
            name = 'true_cont'
            if self.analysis_name is not None:
                name += f'_{self.analysis_name}'
            true_analysis_struct = dir_handlers.AnalysisDir(main_path, self.run_type, name)

        analysis_struct = None
        if not self.no_run_cont_fit_flag:
            name = 'baseline'
            if self.analysis_name is not None:
                name = self.analysis_name
            analysis_struct = dir_handlers.AnalysisDir(main_path, self.run_type, name)

        return analysis_struct, true_analysis_struct, raw_analysis_struct

    def get_global_struct(self):
        global_path = Path(self.export.get('stack_out_dir'))

        raw_global_struct = None
        if self.run_raw_flag:
            name = 'raw'
            if self.analysis_name is not None:
                name += f'_{self.analysis_name}'
            raw_global_struct = dir_handlers.AnalysisDir(global_path, self.run_type, name)

        true_global_struct = None
        if self.run_true_cont_flag:
            name = 'true_cont'
            if self.analysis_name is not None:
                name += f'_{self.analysis_name}'
            true_global_struct = dir_handlers.AnalysisDir(global_path, self.run_type, name)

        global_struct = None
        if not self.no_run_cont_fit_flag:
            name = 'baseline'
            if self.analysis_name is not None:
                name = self.analysis_name
            global_struct = dir_handlers.AnalysisDir(global_path, self.run_type, name)

        return global_struct, true_global_struct, raw_global_struct

    def save_config(self, analysis_struct):
        # Write config file for future reference
        with open(analysis_struct.scripts_dir / 'lyatools_config.ini', 'w') as configfile:
            self.config.write(configfile)
