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

        folder = os.getcwd()
        path_root = os.path.join(folder, 'src', 'config.ini')
        path_curdir = os.path.join(folder, '..', 'config.ini')
        if os.path.exists(path_root):
            self.properties.read(path_root)
        elif os.path.exists(path_curdir):
            self.properties.read(path_curdir)
        else:
            raise "Can not find config.ini"

    def get(self, section):
        if not section in self.properties.sections():
            raise "can not find {0} in config.ini".format(section)
        return self.properties[section]
