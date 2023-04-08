import copy
import configparser
from pathlib import Path

from . import submit_utils, dir_handlers
from lyatools.raw_deltas import make_raw_deltas
from lyatools.quickquasars import run_qq
from lyatools.delta_extraction import make_delta_runs
from lyatools.correlations import make_correlation_runs

CORR_TYPES = {'cf_lya_lya_0_10': 'dmat_lya_lya_0_10', 'cf_lya_lyb_0_10': 'dmat_lya_lyb_0_10',
              'xcf_lya_qso_0_10': 'xdmat_lya_qso_0_10', 'xcf_lyb_qso_0_10': 'xdmat_lyb_qso_0_10'}


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
        self.corr = self.config['picca_corr']
        self.export = self.config['picca_export']

        # Get mock setup
        self.input_dir = self.config['mock_setup'].get('input_dir')
        self.qq_dir = self.config['mock_setup'].get('qq_dir')
        self.analysis_dir = self.config['mock_setup'].get('analysis_dir')

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
        self.run_corr_flag = self.config['control'].getboolean('run_corr')
        self.run_export_flag = self.config['control'].getboolean('run_export')

        self.run_raw_flag = self.config['control'].getboolean('run_raw')
        self.run_true_cont_flag = self.config['control'].getboolean('run_true_continuum')
        self.no_run_cont_fit_flag = self.config['control'].getboolean('no_run_continuum_fitted')

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

            if self.run_raw_flag:
                print('Starting stack and export job for raw deltas.')
                _ = self.stack_correlations(raw_corr_dict, raw_global_struct,
                                            exp_job_ids=raw_exp_job_ids)
                submit_utils.print_spacer_line()

            if self.run_true_cont_flag:
                print('Starting stack and export job for true deltas.')
                _ = self.stack_correlations(true_corr_dict, true_global_struct,
                                            exp_job_ids=true_exp_job_ids)
                submit_utils.print_spacer_line()

            if not self.no_run_cont_fit_flag:
                print('Starting stack and export job for fitted deltas.')
                _ = self.stack_correlations(corr_dict, global_struct, exp_job_ids=exp_job_ids)
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

        # Run correlations
        corr_job_ids = None
        if self.run_corr_flag:
            print(f'Starting correlation jobs for seed {seed}.')
            corr_types, corr_job_ids = self.run_correlations(seed, analysis_struct,
                                                             delta_job_ids=delta_job_ids)
        submit_utils.print_spacer_line()

        # Run export
        corr_files = {}
        job_id = None
        if self.run_export_flag:
            print(f'Starting export jobs for seed {seed}.')
            corr_files, job_id = self.run_export(seed, analysis_struct, corr_types,
                                                 corr_job_ids=corr_job_ids)
        submit_utils.print_spacer_line()

        return corr_files, job_id

    def run_qq(self, seed):
        input_dir = Path(self.input_dir) / f'v9.0.{seed}'
        output_dir = Path(self.qq_dir) / f'v9.0.{seed}'
        print(f'Submitting QQ run for mock v9.0.{seed}')

        qq_job_id = run_qq(self.qq_run_type, seed, self.job.getboolean('test_run'),
                           self.job.getboolean('no_submit'), input_dir, output_dir)

        return qq_job_id

    def run_zcat(self, seed, qq_job_id=None):
        main_path = Path(self.qq_dir) / f'v9.0.{seed}'
        qq_struct = dir_handlers.QQDir(main_path, self.qq_run_type)
        self.save_config(qq_struct)

        # Make the zcat if it does not exist already
        zcat_file = qq_struct.qq_dir / 'zcat.fits'
        if zcat_file.is_file():
            return

        print('Making DESI zcat')
        header = submit_utils.make_header(self.job.get('nersc_machine'), nodes=1, time=0.2,
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

    def run_raw_deltas(self, seed, analysis_struct, zcat_job_id=None):
        main_path = Path(self.qq_dir) / f'v9.0.{seed}'
        qq_struct = dir_handlers.QQDir(main_path, self.qq_run_type)

        zcat_file = qq_struct.qq_dir / 'zcat.fits'
        delta_job_ids = make_raw_deltas(self.input_dir, zcat_file, analysis_struct,
                                        self.job, zcat_job_id=zcat_job_id,
                                        run_lyb_region=self.deltas.getboolean('run_lyb_region'),
                                        delta_lambda=self.deltas.getfloat('delta_lambda'),
                                        max_num_spec=self.deltas.getfloat('max_num_spec'),
                                        use_old_weights=self.deltas.getboolean('use_old_weights'))

        return delta_job_ids

    def run_delta_extraction(self, seed, analysis_struct, true_continuum=False,
                             zcat_job_id=None):
        qq_dir = Path(self.qq_dir) / f'v9.0.{seed}' / f'{self.qq_run_type}'
        zcat_file = qq_dir / 'zcat.fits'

        delta_job_ids = make_delta_runs(self.deltas, self.job, qq_dir, zcat_file, analysis_struct,
                                        zcat_job_id, true_continuum=true_continuum)

        return delta_job_ids

    def run_correlations(self, seed, analysis_struct, delta_job_ids=None):
        qq_dir = Path(self.qq_dir) / f'v9.0.{seed}' / f'{self.qq_run_type}'
        zcat_file = qq_dir / 'zcat.fits'

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

        corr_job_ids = make_correlation_runs(self.corr, self.job, analysis_struct, corr_types,
                                             zcat_file, delta_job_ids)

        return corr_types, corr_job_ids

    def run_export(self, seed, analysis_struct, corr_types, corr_job_ids=None):
        types = copy.deepcopy(corr_types)
        name_string = self.corr.get('name_string', '')
        name_string = '_' + name_string

        # TODO implement other options for redshift bins
        zmin, zmax = 0, 10
        corr_dict = {}
        export_commands = []
        for cf, dmat in types.items():
            file = analysis_struct.corr_dir / f'{cf}_{zmin}_{zmax}{name_string}.fits.gz'
            exp_file = analysis_struct.corr_dir / f'{cf}_{zmin}_{zmax}{name_string}-exp.fits.gz'

            corr_dict[cf] = file
            if not exp_file.is_file():
                # Do the exporting
                command = f'picca_export.py --data {file} --out {exp_file} '

                if self.export.getboolean('add_dmat'):
                    dmat_file = Path(self.export.get('dmat_path')) / f'{dmat}.fits.gz'
                    if not dmat_file.is_file():
                        raise ValueError(f'Asked for dmat, but dmat {dmat_file} could not be found')
                    command += f'--dmat {dmat_file} '

                export_commands += [command]

        if len(export_commands) < 1:
            print(f'No individual mock export needed for seed {seed}.')
            return corr_dict, None

        # Make the header
        header = submit_utils.make_header(self.job.get('nersc_machine'), time=0.2,
                                          omp_threads=64, job_name=f'export_{seed}',
                                          err_file=analysis_struct.logs_dir/f'export-{seed}-%j.err',
                                          out_file=analysis_struct.logs_dir/f'export-{seed}-%j.out')

        # Create the script
        text = header
        env_command = self.job.get('env_command')
        text += f'{env_command}\n\n'
        for command in export_commands:
            text += command + '\n'

        # Write the script.
        script_path = analysis_struct.scripts_dir / f'export-{seed}.sh'
        submit_utils.write_script(script_path, text)

        job_id = submit_utils.run_job(script_path, dependency_ids=corr_job_ids,
                                      no_submit=self.job.getboolean('no_submit'))

        return corr_dict, job_id

    def stack_correlations(self, corr_dict, global_struct, exp_job_ids=None):
        # Stack correlations from different seeds
        export_commands = []
        for cf_name, cf_list in corr_dict.items():
            if len(cf_list) < 1:
                continue

            str_list = [str(cf) for cf in cf_list]
            in_files = ' '.join(str_list)

            exp_out_file = global_struct.corr_dir / f'{cf_name}-exp.fits.gz'

            command = f'lyatools-stack-export --data {in_files} --out {exp_out_file} '

            if self.export.getboolean('add_dmat'):
                dmat = CORR_TYPES[cf_name]
                dmat_file = Path(self.export.get('dmat_path')) / f'{dmat}.fits.gz'
                if not dmat_file.is_file():
                    raise ValueError(f'Asked for dmat, but dmat {dmat_file} could not be found')

                command += f'--dmat {dmat_file} '

                export_commands += [command]

        # Make the header
        header = submit_utils.make_header(self.job.get('nersc_machine'), time=0.2,
                                          omp_threads=64, job_name='stack_export',
                                          err_file=global_struct.logs_dir/'stack_export-%j.err',
                                          out_file=global_struct.logs_dir/'stack_export-%j.out')

        # Create the script
        text = header
        env_command = self.job.get('env_command')
        text += f'{env_command}\n\n'
        for command in export_commands:
            text += command + '\n'

        # Write the script.
        script_path = global_struct.scripts_dir / 'stack_export.sh'
        submit_utils.write_script(script_path, text)

        job_id = submit_utils.run_job(script_path, dependency_ids=exp_job_ids,
                                      no_submit=self.job.getboolean('no_submit'))

        return job_id

    def get_analysis_dirs(self, seed):
        main_path = Path(self.analysis_dir) / f'v9.0.{seed}'

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
