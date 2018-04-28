# -*- mode: python; coding: utf-8 -*-

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
