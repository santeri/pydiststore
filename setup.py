#!/usr/bin/env python
# encoding: utf-8

from distutils.core import setup

setup(name='diststore',
      version='1.0',
      description='Simple hash distribution / caching',
      author='Santeri Hernejärvi',
      author_email='santeri@santeri.se',
      packages=['diststore'],
      scripts=['diststored'],
      requires=['httplib2 (>=0.4.0)'],
     )