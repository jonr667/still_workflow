import logging
import socket


def setup_logger(name, level, filepath):
    hostname = socket.gethostname().split('.')[0]
    logger = logging.getLogger(name)

    format = '%(asctime)s - {0} - %(name)s - %(levelname)s - %(message)s'.format(hostname)

    formating = logging.Formatter(format)
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formating)

    fh = logging.FileHandler(filepath + name + '_' + hostname + '.log')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formating)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = True

    return logger
