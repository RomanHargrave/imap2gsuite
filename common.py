import time

from imapclient import IMAPClient
from threading import Thread

import logging
import atexit

# Thread that sends NOOP to keep the server happy
class KeepAliveWorker(Thread):
    def __init__(self, client):
        self.client = client
        super(KeepAliveWorker, self).__init__()

    def run(self):
        try:
            self.client.noop()
            time.sleep(2)
        except:
            # a lazy way to deal with the inevitable exception on logout()
            # rather than using signaling.
            pass

# Message; holds a single mailpieces id and retrieves contents when asked
class Mailpiece:
    def __init__(self, client, message_id):
        self.client     = client
        self.message_id = message_id
        self._r822      = None
        self._envelope  = None

    @property
    def id(self):
        return self.message_id

    @property
    def envelope(self):
        if self._envelope is None:
            res = self.client.fetch(self.message_id, [u'ENVELOPE'])
            self._envelope = res[self.message_id][u'ENVELOPE']

        return self._envelope

    @property
    def subject(self):
        return self.envelope.subject

    @property
    def r822(self):
        if self._r822 is None:
            res = self.client.fetch(self.message_id, [u'RFC822'])
            self._r822 = res[self.message_id][u'RFC822']

        return self._r822

# Folder; holds a list of mailpieces ids, sorted in ascending order
class Folder:
    def __init__(self, client, name, criteria = u'ALL'):
        self.client   = client
        self.name     = name
        self.criteria = criteria
        self._mailpieces = None

    @property
    def mailpieces(self):
        if self._mailpieces is None:
            self.client.select_folder(self.name)
            self._mailpieces = sorted(map((lambda num: Mailpiece(self.client, num)), self.client.search(self.criteria)), key=lambda x: x.id)

        return self._mailpieces

# Account; holds a list of folder names
class Connection:
    def __init__(self, server, username, password, ssl=True):
        self.server     = server
        self.username   = username
        self.password   = password
        self.use_ssl    = ssl
        self._client    = None
        self._folders   = None

    @property
    def client(self):
        if self._client is None:
            self._client = IMAPClient(host=self.server, ssl=self.use_ssl)
            self._client.login(self.username, self.password)
            atexit.register(self._client.logout)
            #KeepAliveWorker(self._client).start()

        return self._client

    @property
    def folders(self):
        if self._folders is None:
            self._folders = map((lambda data: Folder(self.client, data[2])), self.client.list_folders())

        return self._folders
