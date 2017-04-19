#!/usr/bin/env python

# from distutils.core import setup

from setuptools import setup, find_packages

import dafpy

REQUIREMENTS = []

##############################
# Setup
##############################

setup(name='dafpy',
      version=dafpy.__version__,
      description='A package for dataflow programming with python callables',
      author='Emmanuel Serie',
      author_email='emmanuel.serie@cfm.fr',
      packages=find_packages(),
      install_requires=REQUIREMENTS,
      )
