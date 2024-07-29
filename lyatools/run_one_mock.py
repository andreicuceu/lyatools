import configparser
from pathlib import Path

from . import submit_utils, dir_handlers
from lyatools.raw_deltas import make_raw_deltas
from lyatools.quickquasars import run_qq
from lyatools.delta_extraction import make_delta_runs
from lyatools.qsonic import make_qsonic_runs
from lyatools.correlations import make_correlation_runs
from lyatools.export import make_export_runs, stack_correlations, export_full_cov, mpi_export


MOCK_ANALYSIS_TYPES = [
    'raw',
    'raw_master',
    'true_continuum',
    'continuum_fitted',
]


class MockRun:
    def __init__(self, config, mock_start_path, analysis_start_path, mock_seed, qq_seeds=None):
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

        # Get the mock type
        self.mock_analysis_type = config['control'].get('mock_type')
        if self.mock_analysis_type not in MOCK_ANALYSIS_TYPES:
            raise ValueError(f'Unknown mock analysis type {self.mock_analysis_type}')

        # Get the mock info we need to build the tree
        self.mock_type = config['mock_setup'].get('mock_type')
        skewers_name = config['mock_setup'].get('skewers_name')
        skewers_version = config['mock_setup'].get('skewers_version')
        survey_name = config['mock_setup'].get('survey_name')
        qq_version = config['mock_setup'].get('qq_version')
        qq_run_name = config['mock_setup'].get('qq_run_name')
        spectra_dirname = self.qq_config.get('spectra_dirname')
        analysis_name = config['mock_setup'].get('analysis_name')

        # Initialize the analysis name
        if self.mock_type == 'raw' or self.mock_type == 'raw_master':
            name = 'raw'
            if analysis_name is not None:
                name += f'_{analysis_name}'
        if self.mock_type == 'true_continuum':
            name = 'true_cont'
            if analysis_name is not None:
                name += f'_{analysis_name}'
        if self.mock_type == 'continuum_fitted':
            name = 'baseline' if analysis_name is None else analysis_name

        # Initialize the directory structure
        if self.mock_type == 'raw_master':
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
                mock_start_path, skewers_name, skewers_version, mock_seed,
                survey_name, qq_version, qq_run_name, qq_seeds, spectra_dirname
            )
            self.analysis_tree = dir_handlers.AnalysisTree(
                analysis_start_path, skewers_version, mock_seed, survey_name,
                qq_version, qq_run_name, qq_seeds, name
            )

    def run(self):
        if self.run_qq_flag:
            run_qq(self.qq_tree, self.qq_config, self.job_config, self.mock_type)

        if self.run_deltas_flag:
            make_delta_runs(self.analysis_struct, self.mock_seed)

        if self.run_qsonic_flag:
            make_qsonic_runs(self.analysis_struct, self.mock_seed)

        if self.run_correlations_flag:
            make_correlation_runs(self.analysis_struct, self.mock_seed)

        if self.run_export:
            make_export_runs(self.analysis_struct, self.mock_seed)
