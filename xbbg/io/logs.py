import logging

from xbbg.core import utils

LOG_LEVEL = 'CRITICAL'
LOG_FMT = '%(asctime)s:%(name)s:%(levelname)s:%(message)s'


def get_logger(name_or_func, log_file='', level=LOG_LEVEL, types='stream', **kwargs):
    """
    Generate logger

    Args:
        name_or_func: logger name or current running function
        log_file: logger file
        level: level of logs - debug, info, error
        types: file or stream, or both

    Returns:
        logger

    Examples:
        >>> get_logger(name_or_func='download_data', level='debug', types='stream')
        <Logger download_data (DEBUG)>
        >>> get_logger(name_or_func='preprocess', log_file='pre.log', types='file|stream')
        <Logger preprocess (CRITICAL)>
    """
    if isinstance(level, str): level = getattr(logging, level.upper())
    log_name = name_or_func if isinstance(name_or_func, str) else utils.func_scope(name_or_func)
    logger = logging.getLogger(name=log_name)
    logger.setLevel(level=level)

    if not len(logger.handlers):
        formatter = logging.Formatter(fmt=kwargs.get('fmt', LOG_FMT))

        if 'file' in types:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(fmt=formatter)
            logger.addHandler(file_handler)

        if 'stream' in types:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(fmt=formatter)
            logger.addHandler(stream_handler)

    return logger
