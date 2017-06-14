from random import choice, randint
from pyactor.context import  sleep, interval
from pyactor.exceptions import TimeoutError
from grup import *
from printer import *
from main import *


class Peer(object):
    _tell = ['attach', 'init_start', 'announce_me', 'notify_join', 'multicast', 'bully', 'new_sequencer', 'bully',
             'alive','get_last_processed']
    _ask = ['get_sequencer', 'get_priority', 'vote', 'receive']
    _ref = ['new_sequencer']

    def __init__(self):
        self.cache = {}
        self.members = []
        self.priority = 0
        self.last_processed = 0
        self.messages = []
        self.wait_list = []
        self.eleccions = False

    def lookup_cache(self, url):
        if url == self.url:
            return self.proxy

        try:
            return self.cache[url]
        except KeyError:
            try:
                self.cache[url] = self.host.lookup_url(url, Peer)
                return self.cache[url]
            except TimeoutError:
                self.printer.printer('ERROR lookup_url')

    def attach(self, url, printer):
        #self.printer = self.lookup_cache(printer)
        self.printer = self.host.lookup_url(printer, Print)
        self.grup = self.host.lookup_url(url, Grup)
        members_list = self.grup.join(self.url)
        self.members = list(set(members_list) - set([self.url]))

        for member in self.members:
            mem_proxy = self.lookup_cache(member)
            mem_proxy.notify_join(self.url)

        self.init_start()

        try:
            mem = self.lookup_cache(choice(self.members))
            self.sequencer_url = mem.get_sequencer()
        except Exception:
            self.sequencer_url = self.url

        self.sequencer = self.lookup_cache(self.sequencer_url)

    def notify_join(self, url):
        self.members.append(url)

    def init_start(self):
        self.interval = interval(self.host, 5, self.proxy, 'announce_me')

    def announce_me(self):
        self.grup.announce(self.url)

    def get_sequencer(self):
        return self.sequencer_url

    def get_priority(self):
        self.priority += 1
        return self.priority

    def multicast(self, msg, intent=0):

        if intent == 3:
            self.bully()

        if self.eleccions:
            intent = -1
        while self.eleccions:
            sleep(1)

        try:
            if self.url == self.sequencer_url:
                priority = self.get_priority()
            else:
                priority = self.sequencer.get_priority()


            self.printer.printer('INFO: multicast Try: ' + str(intent) + ' Member: ' + self.id + ' Priority: ' + str(
                priority) + ' Message: ' + msg + ' Sequencer:' + self.sequencer_url)
            sleep(randint(0, 4))
            for mem in self.members:
                mem_proxy = self.lookup_cache(mem)
                alive = mem_proxy.receive(msg, priority, future=True)
                alive.mem = mem
                alive.add_callback('alive')

            self.receive(msg, priority)
        except TimeoutError:
            self.printer.printer('ERROR: get_priority Try: ' + str(
                intent) + ' Member: ' + self.id + ' Message: ' + msg + ' Sequencer:' + self.sequencer_url)
            sleep(3)
            self.proxy.multicast(msg, intent + 1)

    def alive(self, future):
        if not future.result() == 'ALIVE':
            self.members.pop(future.mem)

    def receive(self, msg, priority):
        self.eliminated = []
        if priority == (self.last_processed + 1):
            self.process_msg(msg, priority)
            self.wait_list.sort(key=lambda x: x[1])
            
            for message, prty in self.wait_list:
                if prty == (self.last_processed + 1):
                    self.process_msg(message, prty)
                    self.eliminated.append(prty)
        else:
            self.wait_list.append((msg, priority))

        for index in self.eliminated:
            self.wait_list.pop([x for x, y in enumerate(self.wait_list) if y[1] == index][0])

        self.printer.printer('id: '+self.id+' wait_list: '+str(self.wait_list))
        del self.eliminated[:]

        return 'ALIVE'

    def process_msg(self, msg, priority):
        self.last_processed = priority
        self.messages.append((msg, priority))
        self.printer.printer(self.id+' process_msg: '+ str(self.messages))

    def get_last_processed(self, priority):
    	if self.last_processed < priority:
    		 self.priority = priority
    	else:
    		self.priority = self.last_processed

    def new_sequencer(self, url):
        self.sequencer_url = url
        self.sequencer = self.lookup_cache(self.sequencer_url)
        if self.url != self.sequencer_url:
        	self.sequencer.get_last_processed(self.last_processed)
        else:
        	self.priority=self.last_processed
        	
        self.printer.printer('SUCCESS: new_sequencer Member: ' + self.id + ' New Sequencer: ' + url)
        self.eleccions = False

    def vote(self, id):
        id_a = int(id)
        id_m = int(self.id)
        if id_a < id_m:
            return ('ACCEPT', self.sequencer_url)
        else:
            self.eleccions = True
            return ('DROP', self.sequencer_url)

    def set_sequencer(self):
    	self.new_sequencer(self.url)
        for mem in self.members:
            self.lookup_cache(mem).new_sequencer(self.url)
        

    def bully(self):
        sleep(4)
        self.printer.printer('INFO: Starting elections from ' + self.id)
        ids = [(int(n[22:]), n) for n in self.members if int(n[22:]) > int(self.id)]
        if not ids:
            self.set_sequencer()
        else:
            answer = False
            for id, url in ids:
                try:
                    mem = self.lookup_cache(url)
                    ans, seq_url = mem.vote(self.id)
                    if seq_url != self.sequencer_url:
                        self.new_sequencer(seq_url)
                        answer = True
                        break
                    if ans == 'ACCEPT':
                        mem.bully()
                        answer = True
                        break
                except TimeoutError:
                    answer = True
                    pass
            if not answer:
                self.set_sequencer()
            else:
                self.printer.printer('WARNING: Droping elections ' + self.id)
