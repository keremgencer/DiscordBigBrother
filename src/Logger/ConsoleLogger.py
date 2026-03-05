from datetime import datetime
from Logger.ILogger import ILogger

class ConsoleLogger(ILogger):
    def log(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        full_message = f'[{timestamp}] {message}'
        print(full_message, end="")
