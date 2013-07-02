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

from gate.common.rpc import RpcController, rpc_method


class CaseController(RpcController):

    def __init__(self, conf, exchange, logger=None):
        self.conf = conf
        self.logger = logger
        self.exchange = exchange
        super(CaseController, self).__init__(conf, "case", exchange, logger=logger)

    @rpc_method
    def get_case(self, case=None):
        self.logger.debug("Get case called.")
