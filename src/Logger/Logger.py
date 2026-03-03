from datetime import datetime
from Logger.ILogger import ILogger
import os

_basedir = os.path.dirname(os.path.abspath(__file__))
class Logger(ILogger):
    def __init__(self, log_file=os.path.join(_basedir, "Logger", "activity_log.txt")):
        self.log_file = log_file

    def log(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_message = f'[{timestamp}] {message}'
        print(full_message, end="")  # Konsola da yazdır
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(full_message)