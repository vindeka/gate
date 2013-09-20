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

import traceback
import StringIO

from oslo.config import cfg
from oslo import messaging

from gate.common import log as logging
from gate.process.common.bundle import Bundle
from gate.process.common.pipeline import get_pipeline_driver

LOG = logging.getLogger(__name__)


class ProcessAPI(object):

    target = messaging.Target(version='1.0')

    def __init__(self, server):
        self.server = server

    def _get_pipeline(self, pipeline):
        if not pipeline:
            return None
        pipeline = self.server._pipeline_driver.get(pipeline)
        if not pipeline:
            LOG.error(
                '%s: RPC server missing pipeline with name: %s', self.server.host, pipeline)
            return None
        return pipeline

    def process_url(self, ctx, pipeline, data, url):
        pipeline = self._get_pipeline(pipeline)
        if not pipeline:
            return

        # TODO: Load Stream from the url
        stream = None

        # Perform the actual processing
        bundle = Bundle(self.server, pipeline, data, stream)
        try:
            bundle = pipeline.process(bundle)
        except Exception as e:
            bundle.add_exception(e, traceback=''.join(traceback.format_exc()))

        # Only return the bundle data, bundle is no longer used after this pint
        return bundle.data

    def process_raw(self, ctx, pipeline, data, raw):
        pipeline = self._get_pipeline(pipeline)
        if not pipeline:
            return

        # StringIO for raw data
        stream = StringIO.StringIO(raw)

        # Perform the actual processing
        bundle = Bundle(self.server, pipeline, data, stream)
        try:
            bundle = pipeline.process(bundle)
        except Exception as e:
            bundle.add_exception(e, traceback=''.join(traceback.format_exc()))

        # Only return the bundle data, bundle is no longer used after this pint
        return bundle.data


class ProcessServer(object):

    def __init__(self, host, pipeline_driver=None, topic=None, allow_stop=False):
        topic = topic or 'process'

        self.host = host
        self.topic = 'gate.%s' % topic

        self._pipeline_driver = pipeline_driver
        if not self._pipeline_driver:
        	self._pipeline_driver = get_pipeline_driver()

        self._transport = messaging.get_transport(cfg.CONF)
        self._target = messaging.Target(topic=self.topic, server=self.host)
        self._endpoints = [
            ProcessAPI(self)
        ]
        if allow_stop:
            self._endpoints.append(self)

        self._server = messaging.get_rpc_server(
            self._transport, self._target, self._endpoints)

    def start(self):
        LOG.info(
            '%s: Starting RPC server on target: %s.', self.host, self.topic)
        self._server.start()

    def wait(self):
        self._server.wait()

    def stop(self, ctx):
        LOG.info(
            '%s: Stopping RPC server on target: %s.', self.host, self.topic)
        self._server.start()
        self._server.stop()
        self._server.wait()
