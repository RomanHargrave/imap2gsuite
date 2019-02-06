#!/usr/bin/env python2.7
from common import Connection
import common
import threading
import queue

from datetime import datetime, timedelta

import logging

import io
import os
import sys
import time
import argparse
import traceback

# Google API deps
import apiclient
import httplib2
import oauth2client

import imaplib

SCOPES = [ 'https://www.googleapis.com/auth/apps.groups.migration' ]

def connect_service(args):
    credential_dir = os.path.dirname(args.credential_file)

    if not os.path.isdir(credential_dir):
        os.makedirs(credential_dir)

    cred_store = oauth2client.file.Storage(args.credential_file)
    credentials = cred_store.get()

    if not credentials or credentials.invalid:
        flow = oauth2client.client.flow_from_clientsecrets(args.client_secret_file, SCOPES)
        credentials = oauth2client.tools.run_flow(flow, cred_store, args)

    http = credentials.authorize(httplib2.Http())
    return apiclient.discovery.build('groupsmigration', 'v1', http=http)

class MailProcessor(threading.Thread):
    def __init__(self, log, service, args):
        self.log            = log
        self.mailq          = queue.Queue()
        self.service        = service
        self.alive          = True
        self.args           = args
        self._success_count = 0
        self._failure_count = 0
        super(MailProcessor, self).__init__()

    def qsize(self):
        return self.mailq.qsize()

    @property
    def failure_count(self):
        return self._failure_count

    @property
    def success_count(self):
        return self._success_count

    def submit(self, mailpiece):
        self.mailq.put((mailpiece, 0))

    def stop(self):
        self.alive = False

    def run(self):
        arch            = self.service.archive()
        print_countdown = self.args.print_after

        while self.alive:
            try:
                (message, attempts) = self.mailq.get(block=True,timeout=2);

                self.log.debug('Processing mailpiece %d, attempt number %d' % (message.id, attempts + 1))

                strio = io.BytesIO(message.r822)
                media = apiclient.http.MediaIoBaseUpload(strio, mimetype='message/rfc822')
                time.sleep(1.0 / self.args.rate_limit)
                result = arch.insert(groupId=self.args.group, media_body=media).execute()
                assert result['responseCode'].lower() == 'success'

                self.log.info('Uploaded mailpiece %d after %d attempts; subject=%s' % (message.id, attempts + 1, message.subject))

                self._success_count += 1

            except queue.Empty as qex:
                self.log.debug('Queue miss')
                continue

            except Exception as ex:
                subject = "(error)"
                try:
                    subject = message.subject
                except:
                    pass

                if attempts < self.args.retries:
                    self.log.debug('Message upload failed for message ID %d; re-enqueueing' % (message.id))
                    self.mailq.put((message, attempts + 1))
                else:
                    self.log.exception('Failed to upload message %d after %d attempts; subject=%s' % (message.id, attempts + 1, subject))
                    self._failure_count += 1

            self.mailq.task_done()

            print_countdown -= 1

            if print_countdown == 0:
                print_countdown = self.args.print_after
                self.log.info('... stats: success=%d; failures=%d; queue=%d' % (self.success_count, self.failure_count, self.mailq.qsize()))

def main():
    parser = argparse.ArgumentParser(parents = [oauth2client.tools.argparser], description = 'Migrate an IMAP account to Google Groups')
    parser.add_argument('server',           help='IMAP Server')
    parser.add_argument('username',         help='IMAP Username')
    parser.add_argument('password',         help='IMAP Password')
    parser.add_argument('group',            help='Google Group Address')
    parser.add_argument('--verbose',        dest='verbose', action='store_true', help='Detailed (read: trace level) logging')
    parser.add_argument('--ssl',            dest='use_ssl', action='store_true', help='Use SSL (Requires Valid Cert)')
    parser.add_argument('--ssl-noverify',   dest='verify_ssl', action='store_false', help='Use SSL (Requires Valid Cert)')
    parser.add_argument('--report',         dest='print_after', type=int, default=50, help='Print status every N mailpieces (per folder)')
    parser.add_argument('--rate',           dest='rate_limit', type=int, default=10, help='API Rate Limit')
    parser.add_argument('--cid',            dest='client_secret_file', default='client_secret.json')
    parser.add_argument('--cred-store',     dest='credential_file', default=os.path.join(os.path.expanduser('~'), '.google', 'imap2group.json'))
    parser.add_argument('--imaplib-maxline',dest='maxline', type=int, default=25000, help='Value to set imaplib._MAXLINE to')
    parser.add_argument('--retries',        dest='retries', type=int, default=3, help='Number of retries for a message')
    parser.add_argument('--max-pressure',   dest='qpressure', type=int, default=6000, help='Maximum queue pressure')

    logging.basicConfig(format = '[%(asctime)s %(name)s %(levelname)s] %(message)s')

    log  = logging.getLogger('imap2group')
    args = parser.parse_args()

    imaplib._MAXLINE = args.maxline

    if (args.verbose):
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    connection   = Connection(args.server, args.username, args.password, ssl = args.use_ssl, verify_ssl = args.verify_ssl)
    service      = connect_service(args)
    expect_count = 0
    start_time   = time.time()
    processor    = MailProcessor(log, service, args)
    processor.start()

    for folder in connection.folders:
        try:
            folder_size = len(folder.mailpieces)
            expect_count += folder_size
            log.info('Enqueueing folder %s (%d messages in folder, %d messages currently enqueued)' % (folder.name, folder_size, processor.qsize()))
            for message in folder.mailpieces:
                while processor.qsize() >= args.qpressure:
                    time.sleep(.3)
                processor.submit(message)
        except KeyboardInterrupt as ki:
            log.info('Asking processor to exit')
            processor.stop()
            break;

        except Exception as ex:
            log.exception('Failed to enqueue folder %s. Maybe an IMAP error ocurred?' % (folder.name))

    log.info('Waiting on remaining uploads...');
    processor.join()

    end_time = time.time()
    duration = datetime(1,1,1) + timedelta(seconds=int(end_time - start_time))
    duration_text = '%dd %dh %dm %ds' % (duration.day - 1, duration.hour, duration.minute, duration.second)



    log.info('Summary: ok=%d; fail=%d; total=%d; expected=%d; time=%s' % (processor.success_count, processor.failure_count, processor.success_count + processor.failure_count, expect_count, duration_text))

if __name__ == "__main__":
    main()

