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

import os
import sys
import signal
from re import sub

import eventlet.debug

from oslo.config import cfg

cfg.CONF.register_cli_opts([
    cfg.BoolOpt('daemonize',
                default=True,
                help='Run in daemonize mode.')
])

class Daemon(object):

    """Daemon base class"""

    def run_once(self, *args, **kwargs):
        """Override this to run the script once"""
        raise NotImplementedError('run_once not implemented')

    def run_forever(self, *args, **kwargs):
        """Override this to run forever"""
        raise NotImplementedError('run_forever not implemented')

    def run(self, once=False, **kwargs):
        """Run the daemon"""

        # utils.drop_privileges(self.conf.get('user', 'gate'))
        # utils.capture_stdio(self.logger, **kwargs)

        def kill_children(*args):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            sys.exit()

        signal.signal(signal.SIGTERM, kill_children)
        if once:
            self.run_once(**kwargs)
        else:
            self.run_forever(**kwargs)

