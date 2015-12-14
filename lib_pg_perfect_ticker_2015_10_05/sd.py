# -*- mode: python; coding: utf-8 -*-
#
# Copyright (c) 2015 Andrej Antonov <polymorphm@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

assert str is not bytes

try:
    import systemd.daemon as sd
except ImportError:
    import os
    import socket
    
    class _SdModule:
        def notify(self, status, unset_environment=False):
            assert isinstance(status, str)
            assert isinstance(unset_environment, bool)
            
            notify_socket = os.environ.get('NOTIFY_SOCKET')
            
            if not notify_socket:
                return False
            
            if notify_socket.startswith('@'):
                notify_socket = '\0{}'.format(notify_socket[1:])
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
                    sock.connect(notify_socket)
                    sock.sendall(status.encode())
            except OSError:
                return False
            finally:
                if unset_environment:
                    del os.environ['NOTIFY_SOCKET']
            
            return True
    
    sd = _SdModule()

def notify(*args, **kwargs):
    return sd.notify(*args, **kwargs)
