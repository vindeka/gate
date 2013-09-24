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

from oslo import messaging

from gate.common import log as logging


LOG = logging.getLogger(__name__)


class ProcessClient(object):

	def __init__(self, transport, topic = 'process'):
		self.topic = 'gate.%s' % topic
		self.target = messaging.Target(topic=self.topic, version='1.0')
		self._client = messaging.RPCClient(transport, self.target)

	def process_url(self, evidence_uuid, pipeline, data, url, autosave=True, wait_for_result=False):
		"""Process the url using the given pipeline.
		@param evidence_uuid: Evidence id
		@param pipeline: Name of pipeline to use
		@param data: Current information about the stream
		@param url: Url to identify the stream to load
		@param autosave: Auto passes result onto the engine
		@param wait_for_result: Wait to get the resulting data
		@returns: result data
		"""
		if wait_for_result:
			return self._client.call({}, 'process_url', evidence_uuid=evidence_uuid, pipeline=pipeline,
				data=data, url=url, return_result=wait_for_result, autosave=autosave)
		self._client.cast({}, 'process_url', evidence_uuid=evidence_uuid, pipeline=pipeline,
			data=data, url=url, return_result=wait_for_result, autosave=autosave)

	def process_raw(self, evidence_uuid, pipeline, data, raw, autosave=True, wait_for_result=False):
		"""Process the raw data using the given pipeline.
		@param evidence_uuid: Evidence id
		@param pipeline: Name of pipeline to use
		@param data: Current information about the stream
		@param raw: Raw data to process
		@param autosave: Auto passes result onto the engine
		@param wait_for_result: Wait to get the resulting data
		@returns: result data
		"""
		raw = base64.b64encode(snappy.compress(raw))
		if wait_for_result:
			return self._client.call({}, 'process_raw', evidence_uuid=evidence_uuid, pipeline=pipeline,
				data=data, raw=raw, return_result=wait_for_result, autosave=autosave)
		self._client.cast({}, 'process_raw', evidence_uuid=evidence_uuid, pipeline=pipeline,
			data=data, raw=raw, return_result=wait_for_result, autosave=autosave)

	