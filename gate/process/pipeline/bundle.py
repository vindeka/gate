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

from oslo.config import cfg


class Bundle(object):

    """
    Bundle is passed through the pipeline, contains all information needed to process.
    """

    def __init__(self, server, evidence_uuid, pipeline, data, stream):
        """initialize the bundle.
        @param server: RPC Server instance
        @param evidence_uuid: Evidence id
        @param pipeline: Pipeline processing the object
        @param data: Dictionary of keys for the item
        @param stream: Raw stream of data
        """
        self.server = server
        self.evidence_uuid = evidence_uuid
        self.pipeline = pipeline
        self.data = data
        self.stream = stream

    def publish_obj(self, parent_id, info, data, length=None, wait=False):
        """Publishes the object to be part of the evidence. This piece will
        get processed on the same pipeline.
        @param parent_id: Object's parent id
        @param info: initial object information
        @param data: raw data or file-like object to read data from
        @param length: total amount to read, None reads to end
        @param wait: wait for new id of object
        @returns: created object id
        """
        if not info:
            info = dict()
        info['parent_id'] = parent_id
        return self.server._publish_obj(info, data, length=length, wait=wait)

    def reset(self):
        """Resets the stream to beginning for next module in the pipeline to process."""
        if self.stream:
            if hasattr(self.stream, 'seek'):
                self.stream.seek(0)
            elif hasattr(self.stream, 'reset'):
                self.stream.reset()

    def add_exception(self, exc, traceback=None):
        """Adds an exceptions to the bundle data."""
        if 'exceptions' not in self.data:
            self.data['exceptions'] = list()
        self.data['exceptions'].append({
            'name': exc.__class__.__name__,
            'msg': str(exc),
            'traceback': traceback
        })
