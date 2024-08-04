
from . import submit_utils, dir_handlers
from lyatools.raw_deltas import make_raw_deltas
from lyatools.quickquasars import run_qq, create_qq_catalog, make_contaminant_catalogs
from lyatools.delta_extraction import make_picca_delta_runs
from lyatools.qsonic import make_qsonic_runs
from lyatools.correlations import make_correlation_runs
from lyatools.export import make_export_runs, export_full_cov


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
        self.inject_zerr_config = config['inject_zerr']
        self.qq_config = config['quickquasars']
        self.deltas_config = config['delta_extraction']
        self.qsonic_config = config['qsonic']
        self.corr_config = config['picca_corr']
        self.export_config = config['picca_export']

        # Get control flags
        self.run_qq_flag = config['control'].getboolean('run_qq')
        self.run_zerr_flag = config['control'].getboolean('run_zerr')
        self.run_deltas_flag = config['control'].getboolean('run_deltas')
        self.run_qsonic_flag = config['control'].getboolean('run_qsonic')
        self.run_corr_flag = config['control'].getboolean('run_corr')
        self.run_export_flag = config['control'].getboolean('run_export')

        if self.run_deltas_flag and self.run_qsonic_flag:
            raise ValueError('Cannot run deltas and qsonic at the same time.')

        # Get the mock type
        self.mock_analysis_type = config['control'].get('mock_analysis_type')
        if self.mock_analysis_type not in MOCK_ANALYSIS_TYPES:
            raise ValueError(f'Unknown mock analysis type {self.mock_analysis_type}')

        # Get the mock info we need to build the tree
        self.mock_type = config['mock_setup'].get('mock_type')
        skewers_name = config['mock_setup'].get('skewers_name')
        skewers_version = config['mock_setup'].get('skewers_version')
        survey_name = config['mock_setup'].get('survey_name')
        qq_version = config['mock_setup'].get('qq_version')
        qq_run_type = config['mock_setup'].get('qq_run_type')
        spectra_dirname = self.qq_config.get('spectra_dirname', 'spectra-16')
        analysis_name = config['mock_setup'].get('analysis_name')

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
        else:
            self.qq_tree = dir_handlers.QQTree(
                mock_start_path, skewers_name, skewers_version, mock_seed, survey_name,
                qq_version, qq_run_type, skewers_start_path, qq_seeds, spectra_dirname
            )
            self.analysis_tree = dir_handlers.AnalysisTree(
                analysis_start_path, skewers_version, mock_seed, survey_name,
                qq_version, qq_run_type, qq_seeds, name
            )

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

    def run_mock(self):
        job_id = None
        if self.run_qq_flag:
            job_id = self.run_qq(job_id)

        if self.run_zerr_flag:
            job_id = self.make_zerr_cat(job_id)

        if self.run_deltas_flag or self.run_qsonic_flag:
            job_id = self.run_deltas(job_id)

        corr_paths = None
        if self.run_corr_flag:
            corr_paths, job_id = self.run_correlations(job_id)

        if self.run_export_flag:
            corr_dict, _, __, ___ = self.run_export(corr_paths, job_id)

        return corr_dict, job_id

    def run_qq(self, job_id):
        seed_cat_path = self.qq_tree.qq_dir / "seed_zcat.fits"

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
        job_id, dla_flag, bal_flag = run_qq(
            self.qq_tree, self.qq_config, self.job_config, seed_cat_path,
            self.qq_seed, self.mock_type, job_id
        )

        # Make DLA and BAL catalogs if needed
        if dla_flag or bal_flag:
            job_id = make_contaminant_catalogs(
                self.qq_tree, self.qq_config, self.job_config,
                dla_flag, bal_flag, job_id, run_local=True
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

    def run_deltas(self, qq_job_id):
        qso_cat = self.get_analysis_qso_cat()

        # Run raw deltas
        if 'raw' in self.mock_analysis_type:
            job_id = make_raw_deltas(
                qso_cat, self.qq_tree, self.analysis_tree,
                self.deltas_config, self.job_config, qq_job_id=qq_job_id
            )
            return job_id

        # Get information for the delta extraction
        true_continuum = self.mock_analysis_type == 'true_continuum'

        mask_dla_cat = None
        if self.deltas_config.getboolean('mask_DLAs'):
            mask_nhi_cut = self.qq_config.getfloat('dla_mask_nhi_cut')
            mask_dla_cat = self.qq_tree.qq_dir / f'dla_cat_mask_{mask_nhi_cut:.2f}.fits'

        mask_bal_cat = self.qq_tree.qq_dir / 'bal_cat.fits'
        if self.deltas_config.getboolean('mask_BALs'):
            ai_cut = self.qq.getint('bal_ai_cut', None)
            bi_cut = self.qq.getint('bal_bi_cut', None)
            if not (ai_cut is None and bi_cut is None):
                mask_bal_cat = self.qq_tree.qq_dir / f'bal_cat_AI_{ai_cut}_BI_{bi_cut}.fits'

        # Run picca delta extraction
        if self.run_deltas_flag:
            job_id = make_picca_delta_runs(
                qso_cat, self.qq_tree, self.analysis_tree,
                self.deltas_config, self.job_config, qq_job_id=qq_job_id,
                mask_dla_cat=mask_dla_cat, mask_bal_cat=mask_bal_cat, true_continuum=true_continuum,
            )

            return job_id

        # Run QSOnic
        if self.run_qsonic_flag:
            job_id = make_qsonic_runs(
                qso_cat, self.qq_tree, self.analysis_tree,
                self.deltas_config, self.job_config, qq_job_id=qq_job_id,
                mask_dla_cat=mask_dla_cat, mask_bal_cat=mask_bal_cat, true_continuum=true_continuum,
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
                'Export only runs must include correlation runs as well. '
                'In the [control] section set "run_corr" to True. '
                'Correlations are *not* recomputed if they already exist.'
            )

        corr_dict, job_id, export_commands = make_export_runs(
            corr_paths, self.analysis_tree, self.export_config,
            self.job_config, corr_job_ids=corr_job_ids, run_local=run_local
        )

        job_id_cov = None
        export_cov_commands = None
        if not self.export.getboolean('no_export_full_cov'):
            job_id_cov, export_cov_commands = export_full_cov(
                corr_paths, self.analysis_tree, self.export_config,
                self.job_config, corr_job_ids=corr_job_ids, run_local=run_local
            )

        return corr_dict, [job_id, job_id_cov], export_commands, export_cov_commands

    def get_analysis_qso_cat(self):
        if self.custom_qso_catalog is not None:
            qso_cat = submit_utils.find_path(self.custom_qso_catalog)
        elif self.mock_analysis_type == 'raw_master':
            qso_cat = self.qq_tree.skewers_path / 'master.fits'
            if not qso_cat.is_file():
                raise FileNotFoundError(f'QSO catalog {qso_cat} not found.')
        else:
            no_zerr = not self.inject_zerr_config.getboolean('zerr_in_deltas', False)
            qso_cat = self.get_zcat_path(no_zerr=no_zerr)

        return qso_cat

    def get_zcat_path(self, no_bal_mask=False, no_zerr=False):
        zcat_name = 'zcat'
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
