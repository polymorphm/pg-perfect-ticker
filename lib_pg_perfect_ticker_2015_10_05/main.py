# -*- mode: python; coding: utf-8 -*-
#
# Copyright (c) 2015 Andrei Antonov <polymorphm@gmail.com>
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

import argparse
import asyncio
import signal
import contextlib

from . import pg_perfect_ticker
from . import log
from . import sd

def main():
    parser = argparse.ArgumentParser(
        description='utility for scheduling Postgresql jobs',
    )
    
    parser.add_argument(
        '--check-config-only',
        action='store_true',
        help='do nothing. read and check config only',
    )
    
    parser.add_argument(
        '--not-use-sd-notify',
        action='store_true',
        help='not use sd_notify',
    )
    
    parser.add_argument(
        '--log-config',
        metavar='LOG_CONFIG_PATH',
        help='path to log config file',
    )
    
    parser.add_argument(
        'config',
        nargs='+',
        metavar='CONFIG_PATH',
        help='path to config file',
    )
    
    args = parser.parse_args()
    
    check_config_only = args.check_config_only
    not_use_sd_notify = args.not_use_sd_notify
    log_config_path = args.log_config
    config_path_list = args.config
    
    with contextlib.ExitStack() as stack:
        log.init(log_config_path)
        
        config_ctx = pg_perfect_ticker.ConfigCtx()
        
        pg_perfect_ticker.blocking_read_config(config_ctx, config_path_list)
        
        if check_config_only:
            return
        
        loop = asyncio.get_event_loop()
        ticker_ctx = pg_perfect_ticker.TickerCtx()
        
        ticker_init_fut = loop.create_task(
            pg_perfect_ticker.ticker_init(loop, ticker_ctx, config_path_list, config_ctx),
        )
        
        loop.run_until_complete(ticker_init_fut)
        stack.callback(pg_perfect_ticker.blocking_ticker_shutdown, ticker_ctx)
        
        def shutdown_handler():
            loop.create_task(
                pg_perfect_ticker.ticker_shutdown_handler(loop, ticker_ctx),
            )
        
        loop.add_signal_handler(signal.SIGINT, shutdown_handler)
        stack.callback(loop.remove_signal_handler, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, shutdown_handler)
        stack.callback(loop.remove_signal_handler, signal.SIGTERM)
        
        if not not_use_sd_notify:
            sd.notify('READY=1', unset_environment=True)
        
        ticker_process_fut = loop.create_task(
            pg_perfect_ticker.ticker_process(loop, ticker_ctx),
        )
        
        loop.run_until_complete(ticker_process_fut)
