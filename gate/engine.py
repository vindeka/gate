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

from gate.common.daemon import Daemon
from gate.common.utils import get_logger


class EngineServer(Daemon):

    """Broker between the queue and the database."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='engine-server')
        self.gate_dir = conf.get('gate_dir', '/etc/gate')
        self.amqp_connection = conf.get('amqp_connection',
                'amqp://localhost/')

    def run_forever(self, *args, **kwargs):
        """Run the process continuously."""

        pass

    def run_once(self, *args, **kwargs):
        """Run the process one pass."""

        pass


