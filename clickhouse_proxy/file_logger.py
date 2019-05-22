"""Logs requests to individual files
"""
import time, datetime
import os

class DummyLogger:
    def begin(self, id):
        pass


    def log(self, file_type, message):
        pass


class FileLogger(DummyLogger):
    def __init__(self, log_location):
        self.__id = None
        self.__start = time.process_time()
        self.__start_dt = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.__location = log_location
        if not os.path.isdir(log_location):
            os.mkdir(log_location)


    def begin(self, id):
        self.__id = id
        self.__start = time.process_time()
        self.__start_dt = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.__file_loc = f'{self.__location}/{self.__start_dt}'
        if not os.path.isdir(self.__file_loc):
            os.mkdir(self.__file_loc)


    def log(self, file_type, message):
        now = time.process_time() - self.__start
        if isinstance(message, bytes):
            with open(f'{self.__file_loc}/{file_type}', 'ab') as fp:
                fp.write(f'{now: >9.2f}s:  '.encode('utf-8'))
                fp.write(message)
                fp.write(b"\n")
        else:
            if isinstance(message, dict):
                message = '\n'.join([f'{k}: {v}' for k, v in message.items()])
            with open(f'{self.__file_loc}/{file_type}', 'a') as fp:
                fp.write(f'{now: >9.2f}s:  {message}')
                fp.write("\n")

