#!/usr/bin/env python3

import argparse

from lyatools import submit_utils
from lyatools.run_mock import RunMocks


def main():
    submit_utils.set_umask()
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--config-file", type=str, required=True,
                        help="The path to the lyatools configuration file.")

    args = parser.parse_args()

    mocks = RunMocks(args.config_file)

    mocks.run_mocks()


if __name__ == '__main__':
    main()
