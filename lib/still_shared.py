import logging
import socket
import threading


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


class InputThread(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.myinput = None

    def run(self):
        self.myinput = raw_input()

    def get_user_input(self):
        return self.myinput


def handle_keyboard_input(self, keyboard_input):
    if keyboard_input == 'q':
        print("Exiting cleanly on user request - q pressed")
        self.keep_running = False
        self.shutdown()
    elif keyboard_input == 'p':
        raw_input("The primary process is paused, this does not include threads, press <enter> to continue")
        print("Normal activity has resumed.")
    self.user_input = InputThread()
    self.user_input.start()
