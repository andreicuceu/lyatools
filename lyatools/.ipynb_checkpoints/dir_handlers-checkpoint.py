from typing import Union
from pathlib import Path
from subprocess import call
from dataclasses import dataclass, field

def make_symlink(target, link_name):
    """ Make a symbolic link named link_name pointing to target.
    Args:
        target: str or Path
            Target of the symbolic link.
        link_name: str or Path
            Name of the symbolic link to create.
    """
    link_name = Path(link_name)
    if link_name.exists() or link_name.is_symlink():
        link_name.unlink()
    link_name.symlink_to(target)


def make_permission_group_desi(dir: Path):
    """
    Makes a directory and all its contents have DESI as their permission group.
    Args:
        dir: Path
            Directory to deal with.
    """
    call(f'chgrp -R desi {dir}', shell=True)
    call(f'chmod -R g+rxw {dir}', shell=True)
    call(f'chmod -R g+s {dir}', shell=True)


def check_dir(dir: Path):
    """
    Checks that a directory exists, and that its permission group is DESI.
    Args:
        dir: Path
            Directory to check
    """
    if not dir.is_dir():
        dir.mkdir(parents=True, exist_ok=True)
        make_permission_group_desi(dir)


@dataclass
class QQTree:
    """
    An object that contains the directory structure for mock generation and analysis.
    """
    mock_start_path: Union[str, Path]

    skewers_name: str
    skewers_version: str
    mock_seed: str

    survey_name: str
    qq_version: str
    qq_run_name: str

    skewers_start_path: Union[str, None] = None
    qq_seeds: Union[str, None] = None
    spectra_dirname: str = 'spectra-16'

    skewers_path: Path = field(init=False)
    qq_dir: Path = field(init=False)
    spectra_dir: Path = field(init=False)
    runfiles_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    scripts_dir: Path = field(init=False)
    full_mock_seed: str = field(init=False)

    def __post_init__(self):
        # This is the start point for the mock tree
        # E.g. desi/mocks/lya_forest/london
        mock_start_path = Path(self.mock_start_path)
        if not mock_start_path.is_dir():
            raise RuntimeError(f'The mock start path does not exist: {mock_start_path}')

        if self.skewers_start_path is not None:
            self.skewers_path = Path(self.skewers_start_path)
            if not self.skewers_path.is_dir():
                raise RuntimeError(f'The skewers start path does not exist: {self.skewers_path}')
        else:
            self.skewers_path = mock_start_path

        # This is the path to the skewers for this mock
        # E.g. desi/mocks/lya_forest/london/lyacolore_skewers/v5.9/skewers-0/
        self.skewers_path = self.skewers_path / self.skewers_name / self.skewers_version
        self.skewers_path = self.skewers_path / f'skewers-{self.mock_seed}'
        if not self.skewers_path.is_dir():
            raise RuntimeError(f'The skewers path does not exist: {self.skewers_path}')

        # This is the path to the quickquasars run for this mock
        # E.g. desi/mocks/lya_forest/london/qq_desi_y3/v5.9.4/mock-0/jura-124
        # the mock seed also allows multiple seeds for the quickquasars run (e.g. mock-0.1.0)
        self.full_mock_seed = f'{self.mock_seed}'
        if self.qq_seeds is not None:
            self.full_mock_seed = f'{self.mock_seed}.{self.qq_seeds}'

        self.qq_dir = mock_start_path / self.survey_name
        check_dir(self.qq_dir)
        self.qq_dir = self.qq_dir / f'{self.skewers_version}.{self.qq_version}'
        check_dir(self.qq_dir)
        self.qq_dir = self.qq_dir / f'mock-{self.full_mock_seed}'
        check_dir(self.qq_dir)
        self.qq_dir = self.qq_dir / self.qq_run_name
        check_dir(self.qq_dir)

        # These are the directories needed for the quickquasars run
        self.spectra_dir = self.qq_dir / self.spectra_dirname
        check_dir(self.spectra_dir)
        self.runfiles_dir = self.qq_dir / 'run_files'
        check_dir(self.runfiles_dir)
        self.logs_dir = self.qq_dir / 'logs'
        check_dir(self.logs_dir)
        self.scripts_dir = self.qq_dir / 'scripts'
        check_dir(self.scripts_dir)


@dataclass
class AnalysisTree:

    analysis_start_path: Union[str, Path]

    skewers_version: str
    mock_seed: str
    survey_name: str
    qq_version: str
    qq_run_name: str

    qq_seeds: Union[str, None] = None
    analysis_name: str = 'baseline'

    analysis_dir: Path = field(init=False)
    corr_dir: Path = field(init=False)
    deltas_lya_dir: Path = field(init=False)
    deltas_lyb_dir: Path = field(init=False)
    fits_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    scripts_dir: Path = field(init=False)
    full_mock_seed: str = field(init=False)

    def _init_indirs_from_base(self, base):
        analysis_dir = base / self.survey_name
        check_dir(analysis_dir)
        analysis_dir = analysis_dir / f'{self.skewers_version}.{self.qq_version}'
        check_dir(analysis_dir)
        analysis_dir = analysis_dir / f'analysis-{self.full_mock_seed}'
        check_dir(analysis_dir)
        analysis_dir = analysis_dir / self.qq_run_name
        check_dir(analysis_dir)
        analysis_dir = analysis_dir / self.analysis_name
        check_dir(analysis_dir)
        return analysis_dir

    def _init_outdirs_from_base(
            self, base, corr=True, deltas=True, fits=True,
            logs=True, scripts=True
    ):
        # These are the directories needed for the analysis
        if corr:
            self.corr_dir = base / 'correlations'
            check_dir(self.corr_dir)
        if deltas:
            self.deltas_lya_dir = base / 'deltas_lya'
            check_dir(self.deltas_lya_dir)
            self.deltas_lyb_dir = base / 'deltas_lyb'
            check_dir(self.deltas_lyb_dir)
        if fits:
            self.fits_dir = base / 'fits'
            check_dir(self.fits_dir)
        if logs:
            self.logs_dir = base / 'logs'
            check_dir(self.logs_dir)
        if scripts:
            self.scripts_dir = base / 'scripts'
            check_dir(self.scripts_dir)

    def __post_init__(self):
        # This is the start point for the analysis tree
        # E.g. desi/science/lya/mock_analysis/london
        analysis_start_path = Path(self.analysis_start_path)
        if not analysis_start_path.is_dir():
            raise RuntimeError(f'The analysis start path does not exist: {analysis_start_path}')

        # This is the path to the analysis for this mock
        # E.g. desi/science/lya/mock_analysis/london/qq_desi_y3/v5.9.4/analysis-0/jura-124/baseline
        self.full_mock_seed = f'{self.mock_seed}'
        if self.qq_seeds is not None:
            self.full_mock_seed = f'{self.mock_seed}.{self.qq_seeds}'

        self.analysis_dir = self._init_indirs_from_base(analysis_start_path)
        self._init_outdirs_from_base(self.analysis_dir)

    def newOutputDirs(
            self, newbase, corr=True, deltas=False, fits=True,
            logs=True, scripts=True
    ):
        newbase = self._init_indirs_from_base(Path(newbase))
        self._init_outdirs_from_base(
            newbase, corr, deltas, fits, logs, scripts)

    @classmethod
    def stack_from_other(cls, other, stack_name: str = 'stack'):
        return cls(
            other.analysis_start_path, other.skewers_version, stack_name,
            other.survey_name, other.qq_version, other.qq_run_name, None,
            other.analysis_name
        )

# @dataclass
# class QQDir:
#     """
#     An object that contains the directory structure for a quickquasars run.
#     """
#     main_path: Union[str, Path]
#     qq_dirname: str
#     spectra_dirname: str = 'spectra-16'
#     run_dirname: str = 'run_files'
#     log_dirname: str = 'logs'
#     scripts_dirname: str = 'scripts'

#     qq_dir: Path = field(init=False)
#     spectra_dir: Path = field(init=False)
#     run_dir: Path = field(init=False)
#     log_dir: Path = field(init=False)
#     scripts_dir: Path = field(init=False)

#     def __post_init__(self):
#         main_path = Path(self.main_path)
#         check_dir(main_path)

#         self.qq_dir = main_path / self.qq_dirname
#         check_dir(self.qq_dir)

#         self.spectra_dir = self.qq_dir / self.spectra_dirname
#         check_dir(self.spectra_dir)

#         self.run_dir = self.qq_dir / self.run_dirname
#         check_dir(self.run_dir)

#         self.log_dir = self.qq_dir / self.log_dirname
#         check_dir(self.log_dir)

#         self.scripts_dir = self.qq_dir / self.scripts_dirname
#         check_dir(self.scripts_dir)


# @dataclass
# class AnalysisDir:
#     """
#     An object that contains the directory structure for our analysis.
#     """
#     main_path: Union[str, Path]
#     qq_run_name: str
#     analysis_version: str = 'baseline'
#     deltas_lya_dirname: str = 'deltas_lya'
#     deltas_lyb_dirname: str = 'deltas_lyb'

#     analysis_dir: Path = field(init=False)
#     corr_dir: Path = field(init=False)
#     deltas_lya_dir: Path = field(init=False)
#     deltas_lyb_dir: Path = field(init=False)
#     qsonic_deltas_lya_dir: Path = field(init=False)
#     qsonic_deltas_lyb_dir: Path = field(init=False)
#     fits_dir: Path = field(init=False)
#     logs_dir: Path = field(init=False)
#     scripts_dir: Path = field(init=False)

#     def __post_init__(self):
#         self.main_path = Path(self.main_path)
#         check_dir(self.main_path)

#         main_analysis_dir = self.main_path / self.qq_run_name
#         check_dir(main_analysis_dir)

#         self.analysis_dir = main_analysis_dir / self.analysis_version
#         check_dir(self.analysis_dir)

#         self.corr_dir = self.analysis_dir / 'correlations'
#         check_dir(self.corr_dir)

#         self.deltas_lya_dir = self.analysis_dir / self.deltas_lya_dirname
#         check_dir(self.deltas_lya_dir)

#         self.deltas_lyb_dir = self.analysis_dir / self.deltas_lyb_dirname
#         check_dir(self.deltas_lyb_dir)

#         self.qsonic_deltas_lya_dir = self.analysis_dir / ('qsonic_' + self.deltas_lya_dirname)
#         check_dir(self.qsonic_deltas_lya_dir)

#         self.qsonic_deltas_lyb_dir = self.analysis_dir / ('qsonic_' + self.deltas_lyb_dirname)
#         check_dir(self.qsonic_deltas_lyb_dir)

#         self.fits_dir = self.analysis_dir / 'fits'
#         check_dir(self.fits_dir)

#         self.logs_dir = self.analysis_dir / 'logs'
#         check_dir(self.logs_dir)

#         self.scripts_dir = self.analysis_dir / 'scripts'
#         check_dir(self.scripts_dir)
