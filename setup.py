#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 13 14:51:02 2017

@author: liuxg
"""
from setuptools import find_packages, setup
import os

version = "0.0.1"

readme = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()

req_file = os.path.join(os.path.dirname(__file__), 'requirements.txt')
requirements = [i.strip() for i in open(req_file).readlines()]

setup_params = dict(
    name="pybdrt",
    version=version,
    description="BDRT DBAPI Driver",
    author="liuxg",
    author_email="liuxg@beagledata.com",
    long_description=readme,
    classifiers=[
        "Development Status :: 0.0.1 - Alpha",
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Database :: Front-Ends',
    ],
    keywords='BDRT SQLAlchemy',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects":
            ["bdrt = pybdrt.dialect:BdrtDialect"]
    },
    install_requires=requirements
)

if __name__ == '__main__':
    setup(**setup_params)