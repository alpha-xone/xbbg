import logging

from xbbg.core import utils


def get_logger(
        name_or_func, log_file='', level=logging.INFO,
        fmt='%(asctime)s:%(name)s:%(levelname)s:%(message)s', types='stream'
):
    """
    Generate logger

    Args:
        name_or_func: logger name or current running function
        log_file: logger file
        level: level of logs - debug, info, error
        fmt: log formats
        types: file or stream, or both

    Returns:
        logger

    Examples:
        >>> get_logger('download_data', level='debug', types='stream')
        <Logger download_data (DEBUG)>
        >>> get_logger('preprocess', log_file='xbbg/tests/pre.log', types='file|stream')
        <Logger preprocess (INFO)>
    """
    if isinstance(level, str): level = getattr(logging, level.upper())
    log_name = name_or_func if isinstance(name_or_func, str) else utils.func_scope(name_or_func)
    logger = logging.getLogger(name=log_name)
    logger.setLevel(level=level)

    if not len(logger.handlers):
        formatter = logging.Formatter(fmt=fmt)

        if 'file' in types:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(fmt=formatter)
            logger.addHandler(file_handler)

        if 'stream' in types:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(fmt=formatter)
            logger.addHandler(stream_handler)

    return logger
