import time

class TimeCounter:
    def __init__(self, title):
        self.title = title
        self.start_time = time.time()

    def end(self, content=""):
        print("%s %s : %.5f secs" % (self.title, content, time.time() - self.start_time))