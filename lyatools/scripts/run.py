#!/usr/bin/env python3

import argparse

from lyatools import submit_utils
# from lyatools.run_mock import RunMocks
from lyatools.run_all_mocks import MockBatchRun


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--config-file", type=str, required=True,
                        help="The path to the lyatools configuration file.")

    args = parser.parse_args()

    mocks = MockBatchRun(args.config_file)

    mocks.run()


if __name__ == '__main__':
    main()
