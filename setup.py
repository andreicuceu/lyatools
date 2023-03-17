#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import glob

scripts = glob.glob('bin/*')

requirements = ['numpy', 'scipy', 'astropy', 'numba', 'setuptools', 'cachetools', 'matplotlib']

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Andrei Cuceu",
    author_email='andreicuceu@gmail.com',
    python_requires='>=3.9',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Astronomy',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ],
    description="Some tools and scripts for Lyman-alpha forest analyses.",
    install_requires=requirements,
    license="GNU General Public License v3.0",
    include_package_data=True,
    keywords='lyatools',
    name='lyatools',
    packages=find_packages(include=['lyatools', 'lyatools.*']),
    setup_requires=setup_requirements,
    scripts=scripts,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/andreicuceu/lyatools',
    version='0.1',
)
