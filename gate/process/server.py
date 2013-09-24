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

import base64
import snappy
import traceback
import StringIO

from oslo.config import cfg
from oslo import messaging

from gate.common import log as logging
from gate.process.pipeline import get_pipeline_driver
from gate.process.pipeline.bundle import Bundle
from gate.engine.client import EngineClient

LOG = logging.getLogger(__name__)


class ProcessAPI(object):

    target = messaging.Target(version='1.0')

    def __init__(self, server, engine_client=None):
        self.server = server
        self._engine_client = engine_client

    def _get_pipeline(self, pipeline):
        if not pipeline:
            return None
        pipeline = self.server._pipeline_driver.get(pipeline)
        if not pipeline:
            LOG.error(
                '%s: RPC server missing pipeline with name: %s', self.server.host, pipeline)
            return None
        return pipeline

    def _save_result(self, evidence_uuid, data, wait=False):
        if not self._engine_client:
            LOG.error('Missing engine client, cannot save processing result.')
        obj_id = None
        if 'uuid' in data:
            obj_id = data['uuid']
        
        new_id = self._engine_client.evidence_obj_save(evidence_uuid, data, obj_id=obj_id, wait=wait)
        if wait and new_id:
            data['uuid'] = new_id
        return data

    def process_url(self, ctx, evidence_uuid, pipeline, data, url, return_result=False, autosave=True):
        pipeline = self._get_pipeline(pipeline)
        if not pipeline:
            return

        # TODO: Load Stream from the url
        stream = None

        # Perform the actual processing
        bundle = Bundle(self.server, evidence_uuid, pipeline, data, stream)
        try:
            bundle = pipeline.process(bundle)
        except Exception as e:
            bundle.add_exception(e, traceback=''.join(traceback.format_exc()))

        data = bundle.data
        if autosave:
            data = self._save_result(evidence_uuid, data, wait=return_result)

        if return_result:
            return data

    def process_raw(self, ctx, evidence_uuid, pipeline, data, raw, return_result=False, autosave=True):
        pipeline = self._get_pipeline(pipeline)
        if not pipeline:
            return

        # StringIO for raw data
        stream = StringIO.StringIO(snappy.uncompress(base64.b64decode(raw)))

        # Perform the actual processing
        bundle = Bundle(self.server, evidence_uuid, pipeline, data, stream)
        try:
            bundle = pipeline.process(bundle)
        except Exception as e:
            bundle.add_exception(e, traceback=''.join(traceback.format_exc()))

        data = bundle.data
        if autosave:
            data = self._save_result(evidence_uuid, data, wait=return_result)

        if return_result:
            return data


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

        self._engine_client = EngineClient(self._transport)

        self._endpoints = [
            ProcessAPI(self, engine_client=self._engine_client)
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
