#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) 2013 Vindeka, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages

from gate import __canonical_version__ as version

name = 'gate'

with open('tools/pip-requires', 'r') as f:
    requires = [x.strip() for x in f if x.strip()]

setup(
    name=name,
    version=version,
    description='Forensic Processing for the Cloud',
    license='Apache License (2.0)',
    author='Vindeka, LLC.',
    author_email='dev@vindeka.com',
    url='http://vindeka.com',
    packages=find_packages(exclude=['bin']),
    classifiers=['Development Status :: 1 - Planning',
                 'License :: OSI Approved :: Apache Software License',
                 'Operating System :: POSIX :: Linux',
                 'Programming Language :: Python :: 2.7',
                 'Environment :: No Input/Output (Daemon)'],
    install_requires=requires,
    scripts=['bin/gate-init', 'bin/gate-engine-server',
             'bin/gate-process-server'],
    entry_points={'gate.module_factory': ['hash=gate.modules.hash:module_factory'
                  , 'debug=gate.modules.debug:module_factory'],
                  'gate.transport_factory': ['memcached=gate.transports.memcached:transport_factory'
                  , 'swift=gate.transports.swift:transport_factory']},
    )

