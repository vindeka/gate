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
import uuid
import unittest
import threading
import test

from oslo.config import cfg

from gate.process import ProcessServer
from gate.process.client import ProcessClient
from gate.process.modules.fileinfo import FileInfoModule
from test.unit.gate import BaseTestCase, FakePipeline


TESTFILES = [
    ("test.pdf", b"PDF document, version 1.2", b"application/pdf"),
    ("test.gz", b'gzip compressed data, was "test", from Unix, last modified: '
     b'Sat Jun 28 21:32:52 2008', b"application/x-gzip"),
    ("text.txt", b"ASCII text", b"text/plain"),
    ]


class FileInfoModuleTest(BaseTestCase):

    def __init__(self, *args):
        cfg.CONF(args=[], project='gate', prog='process-server')
        self.enableFakeTransport()
        self.setupLogging()
        super(FileInfoModuleTest, self).__init__(*args)

    def _start_server(self, pipelines=dict(), topic=None, host=None, allow_stop=True):
        server = ProcessServer(host or 'test.host', pipelines, topic=topic, allow_stop=allow_stop)

        thread = threading.Thread(target=server.start)
        thread.daemon = True
        thread.server = server
        thread.start()

        return thread

    def _stop_server(self, client, thread, topic=None):
        client = client._client
        if topic is not None:
            client = client.prepare(topic=topic)
        client.cast({}, 'stop')
        thread.join(timeout=30)

    def test_fileinfo_result(self):
        pipelines = dict()
        pipe = FakePipeline('fileinfo')
        pipe.add_module(FileInfoModule(dict()))
        pipelines['fileinfo'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        for filename, desc, mime in TESTFILES:
            filepath = os.path.join(os.path.dirname(test.__file__),
                "data", filename)

            with open(filepath, 'rb') as fp:
                data = fp.read()

            result = client.process_raw(str(uuid.uuid4()), 'fileinfo',
                {'path': filepath, 'name': filename}, data, autosave=False, wait_for_result=True)

            self.assertEqual(desc, result['filetype'])
            self.assertEqual(mime, result['mimetype'])
            self.assertEqual(len(data), result['filesize'])
            self.assertEqual(os.path.splitext(filename)[1][1:], result['extension'])

        self._stop_server(client, thread)

        

if __name__ == '__main__':
    unittest.main()
