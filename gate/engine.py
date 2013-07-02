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

from gate.common.daemon import Daemon
from gate.common.utils import get_logger
from gate.controllers.rpc import AccountController, CaseController, \
    EvidenceController, ObjectController


class EngineServer(Daemon):

    """Broker between the queue and the database."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='engine-server',
                                 log_to_console=True)
        self.gate_dir = conf.get('gate_dir', '/etc/gate')
        self.account_rpc = AccountController(conf, "engine", self.logger)
        self.case_rpc = CaseController(conf, "engine", self.logger)
        self.evidence_rpc = EvidenceController(conf, "engine", self.logger)
        self.object_rpc = ObjectController(conf, "engine", self.logger)

    def run_forever(self, *args, **kwargs):
        """Run the process continuously."""
        self.account_rpc.consume()

    def run_once(self, *args, **kwargs):
        """Run the process one pass."""
        self.account_rpc.consume(count=1)
