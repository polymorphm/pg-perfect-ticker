# -*- mode: python; coding: utf-8 -*-

import sys
import logging, logging.config
import json

LOGGER_NAME = 'pg-perfect-ticker'

_logger = None

def try_print(*args, **kwargs):
    try:
        return print(*args, **kwargs)
    except OSError:
        pass

def log(level, msg):
    global _logger
    
    if _logger is not None:
        _logger.log(level, msg)
        
        return
    
    if level >= logging.WARNING:
        try_print(msg, file=sys.stderr)
        
        return
    
    try_print(msg)

def init(log_config_path):
    global _logger
    
    if log_config_path is not None:
        with open(log_config_path, encoding='utf-8') as log_fd:
            log_dict = json.load(log_fd)
        logging.config.dictConfig(log_dict)
        _logger = logging.getLogger(LOGGER_NAME)
