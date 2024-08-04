import configparser

from . import submit_utils, dir_handlers
from lyatools.run_one_mock import MockRun
from lyatools.export import stack_correlations


class MockBatchRun:
    def __init__(self, config_path):
        # Read default config and overwrite with input config
        self.config = configparser.ConfigParser()
        self.config.read(submit_utils.find_path('defaults/desi_y5.ini'))
        self.config.read(submit_utils.find_path(config_path))
        self.job_config = self.config['job_info']

        # Get the seeds
        mock_seeds_str = self.config['mock_setup'].get('mock_seeds')
        cat_seeds_str = self.config['mock_setup'].get('cat_seeds')
        qq_seeds_str = self.config['mock_setup'].get('qq_seeds')

        self.mock_seeds = submit_utils.get_seed_list(mock_seeds_str)

        if cat_seeds_str is None and qq_seeds_str is None:
            self.qq_seeds = [None] * len(self.mock_seeds)
        elif cat_seeds_str is None:
            qq_seeds = submit_utils.get_seed_list(qq_seeds_str)
            self.qq_seeds = [f'{seed}.{seed}' for seed in qq_seeds]
        elif qq_seeds_str is None:
            cat_seeds = submit_utils.get_seed_list(cat_seeds_str)
            self.qq_seeds = [f'{seed}.{seed}' for seed in cat_seeds]
        else:
            cat_seeds = submit_utils.get_seed_list(cat_seeds_str)
            qq_seeds = submit_utils.get_seed_list(qq_seeds_str)
            assert len(cat_seeds) == len(qq_seeds)
            self.qq_seeds = [
                f'{cat_seed}.{qq_seed}' for cat_seed, qq_seed in zip(cat_seeds, qq_seeds)]
        assert len(self.mock_seeds) == len(self.qq_seeds)

        # Get the paths
        mock_start_path = submit_utils.find_path(self.config['mock_setup']['mock_start_path'])
        analysis_start_path = submit_utils.find_path(
            self.config['mock_setup']['analysis_start_path'])
        skewers_start_path = submit_utils.find_path(self.config['mock_setup']['skewers_start_path'])

        # Initialize the mock objects
        self.run_mock_objects = []
        for mock_seed, qq_seed in zip(self.mock_seeds, self.qq_seeds):
            self.run_mock_objects.append(
                MockRun(
                    self.config, mock_start_path, analysis_start_path, mock_seed,
                    skewers_start_path=skewers_start_path, qq_seeds=qq_seed
                )
            )

        # Get the run options
        self.run_mocks_individually = self.config['control'].getboolean('run_mocks_individually')
        self.stack_correlations = self.config['control'].getboolean('stack_correlations')

        # Ensure the run options make sense for special cases
        if len(self.run_mock_objects) < 1:
            raise RuntimeError('No mocks to run')
        elif len(self.run_mock_objects) == 1:
            self.run_mocks_individually = True
            self.stack_correlations = False

        self.stack_tree = None
        if self.stack_correlations:
            stack_name = self.config['mock_setup'].get('stack_name', 'stack')
            self.stack_tree = dir_handlers.AnalysisTree.stack_from_other(
                self.run_mock_objects[0].analysis_tree, stack_name
            )

    def run(self):
        corr_dict = {}
        job_ids = []
        if self.run_mocks_individually:
            for mock_obj in self.run_mock_objects:
                submit_utils.print_spacer_line()
                print('Running mock:', mock_obj.analysis_tree.full_mock_seed)

                mock_corr_dict, job_id = mock_obj.run_mock()

                for key, file in mock_corr_dict.items():
                    if key not in corr_dict:
                        corr_dict[key] = []
                    corr_dict[key] += [file]

                if isinstance(job_id, list):
                    job_ids += job_id
                else:
                    job_ids += [job_id]
        else:
            pass

        # Stack mocks
        if self.stack_correlations:
            submit_utils.print_spacer_line()
            name_string = self.config['picca_corr'].get('name_string', None)
            subtract_shuffled = self.config['export'].getboolean('subtract_shuffled')
            _ = stack_correlations(
                    corr_dict, self.stack_tree, self.job_config, shuffled=subtract_shuffled,
                    name_string=name_string, corr_job_ids=job_ids
                )

        submit_utils.print_spacer_line()
        print('All mocks submitted. Done!')
        submit_utils.print_spacer_line()
