import configparser
import copy

from . import submit_utils, dir_handlers
from lyatools.run_one_mock import MockRun
from lyatools.export import stack_correlations, stack_full_covariance, mpi_export
from lyatools.vegafit import run_vega_mpi


class MockBatchRun:
    def __init__(self, config_path):
        # Read default config and overwrite with input config
        self.config = configparser.ConfigParser()
        self.config.optionxform = lambda option: option
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
        dmat_on_first_mock_only = self.config['picca_corr'].getboolean(
            'dmat_on_first_mock_only', False)
        for ii, (mock_seed, qq_seed) in enumerate(zip(self.mock_seeds, self.qq_seeds)):
            this_mock_config = copy.deepcopy(self.config)
            if ii > 0 and dmat_on_first_mock_only:
                this_mock_config['picca_corr']['compute_dmat'] = 'False'

            self.run_mock_objects.append(
                MockRun(
                    this_mock_config, mock_start_path, analysis_start_path, mock_seed,
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
        if self.stack_correlations or not self.run_mocks_individually:
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

                for key, (cf, cf_exp) in mock_corr_dict.items():
                    if key not in corr_dict:
                        corr_dict[key] = [[], []]
                    corr_dict[key][0] += [cf]
                    corr_dict[key][1] += [cf_exp]

                if isinstance(job_id, list):
                    job_ids += job_id
                else:
                    job_ids += [job_id]
        else:
            corr_dict, job_ids = self.run_parallel()

        # Stack mocks
        if self.stack_correlations:
            submit_utils.print_spacer_line()
            name_string = self.config['picca_export'].get('exp_string', None)
            subtract_shuffled = self.config['picca_export'].getboolean('subtract_shuffled')
            _ = stack_correlations(
                    corr_dict, self.stack_tree, self.job_config, shuffled=subtract_shuffled,
                    name_string=name_string, corr_job_ids=job_ids
                )

            no_smooth_covariance_flag = self.config['picca_export'].getboolean(
                'no_smooth_covariance', False)
            cov_string = self.config['picca_export'].get('cov_string', None)
            if cov_string is None and name_string is not None:
                cov_string = name_string
            _ = stack_full_covariance(
                    corr_dict, self.stack_tree, self.job_config,
                    smooth_covariance_flag=not no_smooth_covariance_flag,
                    corr_config=self.run_mock_objects[0].corr_config,
                    name_string=cov_string, corr_job_ids=job_ids
                )

        submit_utils.print_spacer_line()
        print('All mocks submitted. Done!')
        submit_utils.print_spacer_line()

    def run_parallel(self):
        assert not self.run_mocks_individually

        corr_dict = {}
        job_ids = []
        all_export_commands = []
        all_export_cov_commands = []
        all_vega_commands = []
        for mock_obj in self.run_mock_objects:
            submit_utils.print_spacer_line()
            print('Running mock:', mock_obj.analysis_tree.full_mock_seed)

            job_id = None
            if mock_obj.run_lyacolore_flag:
                submit_utils.print_spacer_line()
                job_id = mock_obj.run_lyacolore(job_id)

            if mock_obj.mock_analysis_type == 'raw' or mock_obj.run_qq_flag:
                submit_utils.print_spacer_line()
                # TODO parallelize this
                job_id = mock_obj.create_qq_catalog(job_id, run_local=True)

            if mock_obj.run_qq_flag:
                submit_utils.print_spacer_line()
                job_id = mock_obj.run_qq(job_id)

            if mock_obj.run_zerr_flag:
                submit_utils.print_spacer_line()
                # TODO parallelize this
                job_id = mock_obj.make_zerr_cat(job_id, run_local=True)

            job_id_deltas = None
            if mock_obj.run_deltas_flag or mock_obj.run_qsonic_flag:
                submit_utils.print_spacer_line()
                job_id_deltas = mock_obj.run_deltas(job_id)

            if mock_obj.run_pk1d_flag:
                submit_utils.print_spacer_line()
                job_id = mock_obj.run_pk1d(delta_job_ids=job_id_deltas)

            corr_paths = None
            if mock_obj.run_corr_flag:
                submit_utils.print_spacer_line()
                corr_paths, job_id = mock_obj.run_correlations(job_id_deltas)
            mock_corr_dict = {}
            if mock_obj.run_export_flag:
                submit_utils.print_spacer_line()
                if corr_paths is None:
                    raise ValueError(
                        'Export runs must include correlation runs as well. '
                        'In the [control] section set "run_corr" to True. '
                        'Correlations are *not* recomputed if they already exist.'
                    )
                mock_corr_dict, _, export_commands, export_cov_commands = mock_obj.run_export(
                    corr_paths, job_id, run_local=False)

                for key, (cf, cf_exp) in mock_corr_dict.items():
                    if key not in corr_dict:
                        corr_dict[key] = [[], []]
                    corr_dict[key][0] += [cf]
                    corr_dict[key][1] += [cf_exp]

                if export_commands is not None:
                    all_export_commands += export_commands
                if export_cov_commands is not None:
                    all_export_cov_commands += export_cov_commands

            if mock_obj.run_vega_flag:
                submit_utils.print_spacer_line()
                if not mock_corr_dict:
                    raise ValueError(
                        'Vega runs must include correlation and export runs as well. '
                        'In the [control] section set "run_corr" and "run_export" to True. '
                        'Correlations are *not* recomputed if they already exist.'
                    )
                _, vega_command = mock_obj.run_vega(mock_corr_dict, job_id, run_local=False)

                if vega_command is not None:
                    all_vega_commands += [vega_command]

            if isinstance(job_id, list):
                job_ids += job_id
            else:
                job_ids += [job_id]

        assert self.stack_tree is not None

        export_job_ids = None
        if self.run_mock_objects[0].run_export_flag:
            export_job_ids = mpi_export(
                all_export_commands, all_export_cov_commands,
                self.stack_tree, self.job_config, job_ids
            )

        if self.run_mock_objects[0].run_vega_flag:
            if len(all_vega_commands) < 1:
                print('No vega commands to run.')
            else:
                run_vega_mpi(
                    all_vega_commands, self.stack_tree, self.run_mock_objects[0].vega_config,
                    self.job_config, export_job_ids
                )

        return corr_dict, job_ids
