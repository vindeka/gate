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

import sys
import socket

from oslo.config import cfg
from gate.common.daemon import Daemon
from gate.common import log as logging
from gate.engine.server import EngineServer


LOG = logging.getLogger(__name__)


cfg.CONF.register_cli_opts([
    cfg.StrOpt('host',
               default=socket.gethostname(),
               help='Name of this node.  This can be an opaque identifier.  '
               'It is not necessarily a hostname, FQDN, or IP address. '
               'However, the node name must be valid within '
               'an AMQP key, and if using ZeroMQ, a valid '
               'hostname, FQDN, or IP address'),
])


class Server(Daemon):

    def __init__(self, host):
        self.host = host
        self.rpc_server = EngineServer(self.host, self.pipelines)

    def start(self):
        """Starts the rpc server."""
        self.rpc_server.start()

    def wait(self):
        """Waits for rpc server to end."""
        self.rpc_server.wait()

    def stop(self):
        """Stops the rpc server"""
        self.rpc_server.stop()

    def run_forever(self, *args, **kwargs):
        """Run the process continuously."""
        self.start()
        try:
            self.wait()
        except:
            self.stop()
            raise
        self.stop()

    def run_once(self, *args, **kwargs):
        """Run the process one pass."""
        self.start()
        try:
            self.wait()
        except:
            self.stop()
            raise
        self.stop()


def main(argv=None, host=None, once=False, **kwargs):
    if argv is None:
        argv = sys.argv
    
    cfg.CONF(argv[1:], project='gate')
    logging.setup('gate')

    # once on command line (i.e. daemonize=false) will over-ride config
    once = once or not cfg.CONF.daemonize

    if not host:
        host = cfg.CONF.host

    try:
        Server(host).run(once=once, **kwargs)
    except KeyboardInterrupt:
        LOG.info('%s: User quit.', host)
    LOG.info('%s: Exited.', host)
