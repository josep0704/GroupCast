from datetime import datetime
from random import sample
from pyactor.context import * #set_context, create_host, serve_forever, interval


class Grup(object):
    _tell = ['announce', 'calcul_time', 'init_intervals', 'leave']
    _ask = ['join']

    def __init__(self):
        self.grup = {}

    def init_intervals(self):
        self.interval = interval(self.host, 10, self.proxy, "calcul_time")

    def join(self, url):
        self.announce(url)
        return self.grup.keys()

    def announce(self, idPeer):
        # print idPeer, datetime.now()
        self.grup[idPeer] = datetime.now()

    def leave(self, p):
        del self.grup[p]

    def calcul_time(self):
        # print datetime.now(), self.grup.keys()
        timeNow = datetime.now()
        for p, ttl in self.grup.items():
            resta = timeNow - ttl
            if (resta.total_seconds() > 10):
                self.leave(p)

if __name__ == "__main__":
    set_context()
    host = create_host('http://127.0.0.1:1800/')
    print 'Created Host: http://127.0.0.1:1800'
    grup = host.spawn('grup',Grup)
    grup.init_intervals()

    serve_forever()