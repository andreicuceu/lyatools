from pathlib import Path

from . import submit_utils, dir_handlers
from lyatools.lyacolore import run_lyacolore
from lyatools.raw_deltas import make_raw_deltas
from lyatools.quickquasars import run_qq, create_qq_catalog, make_catalogs
from lyatools.delta_extraction import make_picca_delta_runs
from lyatools.qsonic import make_qsonic_runs
from lyatools.correlations import make_correlation_runs
from lyatools.pk1d import make_pk1d_runs
from lyatools.export import make_export_runs, export_full_cov
from lyatools.vegafit import make_vega_config
from lyatools import qq_run_args


MOCK_ANALYSIS_TYPES = [
    'raw',
    'raw_master',
    'true_continuum',
    'continuum_fitted',
]


class MockRun:
    def __init__(
        self, config, mock_start_path, analysis_start_path, mock_seed,
        skewers_start_path=None, qq_seeds=None
    ):
        # Get individual configs
        self.job_config = config['job_info']
        self.lyacolore= config['lyacolore']
        self.inject_zerr_config = config['inject_zerr']
        self.qq_config = config['quickquasars']
        self.deltas_config = config['delta_extraction']
        self.qsonic_config = config['qsonic']
        self.corr_config = config['picca_corr']
        self.pk1d_config = config['picca_Pk1D']
        self.export_config = config['picca_export']
        self.vega_config = {key: config[key] for key in config.keys() if key.startswith('vega')}

        # Get control flags
        self.run_lyacolore_flag = config['control'].getboolean('run_lyacolore')
        self.run_qq_flag = config['control'].getboolean('run_qq')
        self.run_zerr_flag = config['control'].getboolean('run_zerr')
        self.run_deltas_flag = config['control'].getboolean('run_deltas')
        self.run_qsonic_flag = config['control'].getboolean('run_qsonic')
        self.run_corr_flag = config['control'].getboolean('run_corr')
        self.run_pk1d_flag = config['control'].getboolean('run_pk1d')
        self.run_export_flag = config['control'].getboolean('run_export')
        self.run_vega_flag = config['control'].getboolean('run_vega')
        self.only_qso_targets_flag = config['quickquasars'].getboolean('only_qso_targets')


        if self.run_deltas_flag and self.run_qsonic_flag:
            raise ValueError('Cannot run deltas and qsonic at the same time.')

        # Get the mock type
        self.mock_analysis_type = config['control'].get('mock_analysis_type')
        if self.mock_analysis_type not in MOCK_ANALYSIS_TYPES:
            raise ValueError(f'Unknown mock analysis type {self.mock_analysis_type}')

        # Get the mock info we need to build the tree
        self.mock_type = config['mock_setup'].get('mock_type')
        self.mock_setup = config['mock_setup']
        skewers_name = config['mock_setup'].get('skewers_name')
        skewers_version = config['mock_setup'].get('skewers_version')
        survey_name = config['mock_setup'].get('survey_name')
        qq_version = config['mock_setup'].get('qq_version')
        qq_run_type = config['mock_setup'].get('qq_run_type')
        spectra_dirname = self.qq_config.get('spectra_dirname', 'spectra-16')
        analysis_name = config['mock_setup'].get('analysis_name')
        newoutputdir = config['mock_setup'].get('new_output_dir', None)

        # Get the paths from pre-existing runs
        preexisting_analysis_start_path = config['picca_corr'].get('preexisting_analysis_start_path', None)
        if preexisting_analysis_start_path is None:
            # Assume same as analysis start path
            preexisting_analysis_start_path = analysis_start_path
        use_preexisting_analysis_deltas = config['picca_corr'].get('use_preexisting_analysis_deltas', None)

        # Initialize the analysis name
        if self.mock_analysis_type == 'raw' or self.mock_analysis_type == 'raw_master':
            name = 'raw'
            if analysis_name is not None:
                name += f'_{analysis_name}'
        if self.mock_analysis_type == 'true_continuum':
            name = 'true_cont'
            if analysis_name is not None:
                name += f'_{analysis_name}'
        if self.mock_analysis_type == 'continuum_fitted':
            name = 'baseline' if analysis_name is None else analysis_name

        # Initialize the directory structure
        if self.mock_analysis_type == 'raw_master':
            assert not self.run_qq_flag
            assert not self.run_zerr_flag
            assert not self.run_qsonic_flag
            self.qq_tree = None
            self.analysis_tree = dir_handlers.AnalysisTree(
                analysis_start_path, skewers_version, mock_seed,
                'raw_master', '0', 'raw_master', None, name
            )

            if self.run_lyacolore_flag:
                raw_path = Path(self.lyacolore.get('lyacolore_dir'))
            elif skewers_start_path is None:
                raw_path = Path(mock_start_path)
            else:
                raw_path = Path(skewers_start_path)

            self.skewers_path = raw_path / skewers_name / skewers_version / f'skewers-{mock_seed}'
            self.raw_master_qso_cat = self.skewers_path / 'master.fits'

            if not self.raw_master_qso_cat.is_file():
                raise FileNotFoundError(f'QSO catalog {self.raw_master_qso_cat} not found.')

        else:
            self.qq_tree = dir_handlers.QQTree(
                mock_start_path, skewers_name, skewers_version, mock_seed, survey_name,
                qq_version, qq_run_type, skewers_start_path, qq_seeds, spectra_dirname
            )
            self.analysis_tree = dir_handlers.AnalysisTree(
                analysis_start_path, skewers_version, mock_seed, survey_name,
                qq_version, qq_run_type, qq_seeds, name
            )

            if newoutputdir is not None:
                self.analysis_tree.newOutputDirs(newoutputdir)

        if not self.run_deltas_flag and use_preexisting_analysis_deltas is not None:
            preexisting_tree = dir_handlers.AnalysisTree(
                preexisting_analysis_start_path, skewers_version, mock_seed, survey_name,
                qq_version, qq_run_type, qq_seeds, use_preexisting_analysis_deltas
            )
            dir_handlers.make_symlink(preexisting_tree.deltas_lya_dir/'Delta',self.analysis_tree.deltas_lya_dir/'Delta')
            dir_handlers.make_symlink(preexisting_tree.deltas_lyb_dir/'Delta',self.analysis_tree.deltas_lyb_dir/'Delta')
            dir_handlers.make_symlink(preexisting_tree.deltas_lya_dir/'Log',self.analysis_tree.deltas_lya_dir/'Log')
            dir_handlers.make_symlink(preexisting_tree.deltas_lyb_dir/'Log',self.analysis_tree.deltas_lyb_dir/'Log')
            del preexisting_tree
        
        # Figure out the seeds
        self.qq_cat_seed = mock_seed
        self.qq_seed = mock_seed
        if qq_seeds is not None:
            qq_seeds_list = qq_seeds.split('.')
            assert len(qq_seeds_list) == 2
            self.qq_cat_seed = int(qq_seeds_list[0])
            self.qq_seed = int(qq_seeds_list[1])

        self.masked_bal_qso_flag = self.qq_config.getboolean('masked_bal_qso')
        self.custom_qso_catalog = config['mock_setup'].get('custom_qso_catalog')

        self.bal_flag = False
        self.dla_flag = False
        self.qq_special_args = None
        if self.mock_analysis_type != 'raw_master':
            self.qq_special_args, self.bal_flag, self.dla_flag = self.get_qq_special_args()

    def run_mock(self):
        job_id = None

        if self.run_lyacolore_flag:
            submit_utils.print_spacer_line()
            job_id = self.run_lyacolore(job_id)

        if self.run_qq_flag:
            submit_utils.print_spacer_line()
            job_id = self.run_qq(job_id)


        if self.run_zerr_flag:
            submit_utils.print_spacer_line()
            job_id = self.make_zerr_cat(job_id)

        if self.run_deltas_flag or self.run_qsonic_flag:
            submit_utils.print_spacer_line()
            job_id_deltas = self.run_deltas(job_id)

        if self.run_pk1d_flag:
            submit_utils.print_spacer_line()
            job_id = self.run_pk1d(job_id_deltas)

        corr_paths = None
        if self.run_corr_flag:
            submit_utils.print_spacer_line()
            corr_paths, job_id = self.run_correlations(job_id_deltas)

        corr_dict = {}
        if self.run_export_flag:
            submit_utils.print_spacer_line()
            corr_dict, _, __, ___ = self.run_export(corr_paths, job_id)

        if self.run_vega_flag:
            submit_utils.print_spacer_line()
            job_id, _ = self.run_vega(corr_dict, job_id)

        return corr_dict, job_id


    def run_lyacolore(self, job_id):
        if self.run_lyacolore_flag: 
            lyacolore_job_id = run_lyacolore(self.mock_setup, self.lyacolore, self.job_config)
        else:
            lyacolore_job_id = None
        submit_utils.print_spacer_line()
        
        return lyacolore_job_id
        


    def run_qq(self, job_id):
        seed_cat_path = self.qq_tree.qq_dir / "seed_zcat.fits"
        assert self.qq_special_args is not None

        # Make QQ input catalog
        if seed_cat_path.is_file():
            print(f'Found QQ input catalog: {seed_cat_path}. Skipping gen_qso_catalog.')
        else:
            job_id = create_qq_catalog(
                self.qq_tree, seed_cat_path, self.qq_config, self.job_config,
                self.qq_cat_seed, run_local=True
            )

        # TODO Figure out a way to check if QQ run already exists
        # Run quickquasars
        submit_utils.print_spacer_line()
        check_spectra_files = list(self.qq_tree.spectra_dir.glob("*/*/spectra-*.fits*"))
        if len(check_spectra_files) < 1:
            job_id = run_qq(
                self.qq_tree, self.qq_config, self.job_config, seed_cat_path,
                self.qq_seed, self.qq_special_args, prev_job_id=job_id
            )
        else:
            print(f'Found spectra files in {self.qq_tree.spectra_dir}. Skipping quickquasars.')

        # Make QSO, DLA, BAL catalogs
        submit_utils.print_spacer_line()
        job_id = make_catalogs(
            self.qq_tree, self.qq_config, self.job_config,
            self.dla_flag, self.bal_flag, job_id, self.only_qso_targets_flag
        )

        return job_id

    def make_zerr_cat(self, qq_job_id, run_local=True):
        distribution = self.inject_zerr_config.get('distribution')
        amplitude = self.inject_zerr_config.get('amplitude')

        zcat_file = self.get_zcat_path(no_zerr=True)
        zcat_zerr_file = self.get_zcat_path()

        if zcat_zerr_file.is_file():
            return qq_job_id

        command = f'lyatools-add-zerr -i {zcat_file} -o {zcat_zerr_file} '
        command += f'-a {amplitude} -t {distribution} -s {self.qq_seed}\n\n'

        if not run_local:
            return command

        print('Submitting inject zerr job')
        header = submit_utils.make_header(
            self.job_config.get('nersc_machine'), nodes=1, time=0.2,
            omp_threads=128, job_name=f'zerr_{self.qq_tree.mock_seed}',
            err_file=self.qq_tree.runfiles_dir/'run-zerr-%j.err',
            out_file=self.qq_tree.runfiles_dir/'run-zerr-%j.out'
        )

        env_command = self.job_config.get('env_command')
        text = header + f'{env_command}\n\n' + command

        script_path = self.qq_tree.scripts_dir / 'inject_zerr.sh'
        submit_utils.write_script(script_path, text)

        zerr_job_id = submit_utils.run_job(
            script_path, dependency_ids=qq_job_id,
            no_submit=self.job_config.getboolean('no_submit')
        )

        return zerr_job_id

    def run_deltas(self, lyacolore_job_id, qq_job_id):
        no_zerr = not self.inject_zerr_config.getboolean('zerr_in_deltas', False)
        qso_cat = self.get_analysis_qso_cat(no_zerr=no_zerr)

        # Run raw deltas
        if 'raw' in self.mock_analysis_type: 
            if 'master' in self.mock_analysis_type:
                skewers_path = self.skewers_path
            else:
                skewers_path = self.qq_tree.skewers_path

            job_id = make_raw_deltas(
                qso_cat, skewers_path, self.analysis_tree, self.deltas_config, self.mock_type,
                self.job_config, lyacolore_job_id=lyacolore_job_id, qq_job_id=qq_job_id
            )
            return job_id

        # Get information for the delta extraction
        true_continuum = self.mock_analysis_type == 'true_continuum'

        mask_dla_cat = None
        if self.deltas_config.getboolean('mask_DLAs'):
            mask_nhi_cut = self.qq_config.getfloat('dla_mask_nhi_cut')
            mask_snr_cut = self.qq_config.getfloat('dla_mask_snr_cut')
            completeness = self.qq_config.getfloat('dla_completeness')

            dla_cat_name = f'dla_cat_nhi_{mask_nhi_cut:.2f}_snr_{mask_snr_cut:.1f}'
            dla_cat_name += f'_completeness_{completeness:.2f}.fits'
            mask_dla_cat = self.qq_tree.qq_dir / dla_cat_name

        mask_bal_cat = self.qq_tree.qq_dir / 'bal_cat.fits'
        if self.deltas_config.getboolean('mask_BALs'):
            ai_cut = self.qq_config.getint('bal_ai_cut', None)
            bi_cut = self.qq_config.getint('bal_bi_cut', None)
            if not (ai_cut is None and bi_cut is None):
                mask_bal_cat = self.qq_tree.qq_dir / f'bal_cat_AI_{ai_cut}_BI_{bi_cut}.fits'

        # Run picca delta extraction
        if self.run_deltas_flag:
            job_id = make_picca_delta_runs(
                qso_cat, self.qq_tree, self.analysis_tree,
                self.deltas_config, self.job_config, lyacolore_job_id=lyacolore_job_id, qq_job_id=qq_job_id,
                mask_dla_cat=mask_dla_cat, mask_bal_cat=mask_bal_cat, true_continuum=true_continuum,
            )

            return job_id

        # Run QSOnic
        if self.run_qsonic_flag:
            job_id = make_qsonic_runs(
                qso_cat, self.qq_tree, self.analysis_tree,
                self.qsonic_config, self.job_config, qq_job_id=qq_job_id,
                mask_dla_cat=mask_dla_cat, mask_bal_cat=mask_bal_cat, true_continuum=true_continuum,
            )

            return job_id
        
    def run_pk1d(self, delta_job_ids):
        job_id = make_pk1d_runs(
            self.analysis_tree, self.pk1d_config, self.job_config,
            delta_job_ids=delta_job_ids
        )
        return job_id
    
    def run_correlations(self, delta_job_ids):
        qso_cat = self.get_analysis_qso_cat()

        corr_types = []
        if self.corr_config.getboolean('run_auto'):
            corr_types += ['lya_lya']
            if self.corr_config.getboolean('run_lyb_region'):
                corr_types += ['lya_lyb']

        if self.corr_config.getboolean('run_cross'):
            corr_types += ['lya_qso']
            if self.corr_config.getboolean('run_lyb_region'):
                corr_types += ['lyb_qso']

        if len(corr_types) < 1:
            raise ValueError(
                'Run correlations did not find anything to run. Add "run_auto" and/or "run_cross".')

        corr_paths, corr_job_ids = make_correlation_runs(
            qso_cat, self.analysis_tree, self.corr_config,
            self.job_config, corr_types, delta_job_ids
        )

        return corr_paths, corr_job_ids

    def run_export(self, corr_paths, corr_job_ids, run_local=True):
        if corr_paths is None:
            raise ValueError(
                'Export runs must include correlation runs as well. '
                'In the [control] section set "run_corr" to True. '
                'Correlations are *not* recomputed if they already exist.'
            )

        corr_dict, job_id, export_commands = make_export_runs(
            corr_paths, self.analysis_tree, self.export_config,
            self.job_config, corr_job_ids=corr_job_ids, run_local=run_local
        )

        job_id_cov = None
        export_cov_commands = None
        if not self.export_config.getboolean('no_export_full_cov'):
            job_id_cov, export_cov_commands = export_full_cov(
                corr_paths, self.analysis_tree, self.export_config,
                self.job_config, corr_job_ids=corr_job_ids, run_local=run_local
            )

        return corr_dict, [job_id, job_id_cov], export_commands, export_cov_commands

    def run_vega(self, corr_dict, export_job_id, run_local=True):
        if not corr_dict:
            raise ValueError(
                'Vega runs must include correlation and export runs as well. '
                'In the [control] section set "run_corr" and "run_export" to True. '
                'Correlations are *not* recomputed if they already exist.'
            )

        qso_cat = self.get_analysis_qso_cat()

        job_id, command = make_vega_config(
            corr_dict, self.analysis_tree, qso_cat, self.vega_config,
            self.job_config, export_job_id, run_local=run_local
        )

        return job_id, command

    def get_analysis_qso_cat(self, no_zerr=False):
        if self.custom_qso_catalog is not None:
            qso_cat = submit_utils.find_path(self.custom_qso_catalog)
        elif self.mock_analysis_type == 'raw_master':
            qso_cat = self.raw_master_qso_cat
        else:
            qso_cat = self.get_zcat_path(no_zerr=no_zerr)

        return qso_cat

    def get_zcat_path(self, no_bal_mask=False, no_zerr=False):                        
        zcat_name = 'zcat'
        if self.only_qso_targets_flag:
            zcat_name = 'zcat_only_qso_targets'

        if self.masked_bal_qso_flag and (not no_bal_mask):
            ai_cut = self.qq_config.getint('bal_ai_cut', None)
            bi_cut = self.qq_config.getint('bal_bi_cut', None)

            if not (ai_cut is None and bi_cut is None):
                zcat_name += f'_masked_AI_{ai_cut}_BI_{bi_cut}'

        if self.run_zerr_flag and (not no_zerr):
            distribution = self.inject_zerr_config.get('distribution')
            amplitude = self.inject_zerr_config.get('amplitude')
            zcat_name += f'_{distribution}_{amplitude}'

        zcat_file = self.qq_tree.qq_dir / (zcat_name + '.fits')
        return zcat_file

    def get_qq_special_args(self):
        split_qq_run_type = self.qq_tree.qq_run_name.split('-')
        if '_' in split_qq_run_type[1]:
            raise ValueError(
                f'Cannot have underscores in middle part of qq_run_type: {split_qq_run_type[1]}'
                ' Please separate the control digits from the rest of the name with dash lines.'
                ' E.g. Use jura-124-test instead of jura-124_test'
            )

        qq_special_args = []
        for digit in split_qq_run_type[1]:
            if digit == '0':
                assert len(split_qq_run_type[1]) == 1
                continue

            if digit not in qq_run_args.QQ_RUN_CODES:
                raise ValueError(
                    f'Invalid digit {digit} in qq_run_type: {split_qq_run_type[1]}.'
                    ' Only the following digits are currently recognized:'
                    f' {qq_run_args.QQ_RUN_CODES}. Please add the new digit to the'
                    ' QQ_RUN_CODES dictionary in qq_run_args.py'
                )

            qq_special_args += [qq_run_args.QQ_RUN_CODES[digit]]

            if digit == '2':
                metal_strengths = self.qq_config.get('metal_strengths')
                if metal_strengths is None:
                    metal_strengths = qq_run_args.QQ_DEFAULT_METAL_STRENGTHS[self.mock_type]
                qq_special_args += [f'--metal-strengths {metal_strengths}']

        dla_flag = '1' in split_qq_run_type[1]
        bal_flag = '4' in split_qq_run_type[1]

        return qq_special_args, bal_flag, dla_flag
