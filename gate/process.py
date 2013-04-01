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
from gate.common.pipeline import Pipelines
from gate.common.objs import FileDataObject

class Process(object):
    """
    Process object that is passed through the pipeline so information about the
    processing processs can be published.
    """

    def __init__(self, conn, pipeline):
        self.connection = conn
        self.pipeline = pipeline

    def process(self, obj):
        self.pipeline.process(self, obj)

    def publish_obj(self, type, name, path, attrs):
        """ Published a newly identified object. """
        pass

class ProcessServer(Daemon):
    """Process objects as the appear in the queue."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='process-server')
        self.gate_dir = conf.get('gate_dir', '/etc/gate')
        self.amqp_connection = conf.get('amqp_connection', 'amqp://localhost/')
        self.pipelines = Pipelines(self.gate_dir)

    def run_forever(self, *args, **kwargs):
        """Run the process continuously."""

        self.logger.debug('%d piplines initilized.' % len(self.pipelines))

        """ TESTING ONLY """
        f = open('/mach_kernel', 'rb')
        obj = FileDataObject(1, 'file', 'mach_kernel', '/mach_kernel', f)
        
        process = Process(None, self.pipelines['testing'])
        process.process(obj)

        print 'Finished Processing'


    def run_once(self, *args, **kwargs):
        """Run the process one pass."""
        pass
