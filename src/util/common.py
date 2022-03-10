import time, os
import configparser as parser

class TimeCounter:
    def __init__(self, title):
        self.title = title
        self.start_time = time.time()

    def end(self, content=""):
        print("%s %s : %.5f secs" % (self.title, content, time.time() - self.start_time))

class Config:
    def __init__(self):
        self.properties = parser.ConfigParser()

        src_folder = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
        config_path = os.path.join(src_folder, 'config.ini')
        if os.path.exists(config_path):
            self.properties.read(config_path)
        else:
            raise "Can not find config.ini"

    def get(self, section):
        if not section in self.properties.sections():
            raise "can not find {0} in config.ini".format(section)
        return self.properties[section]
