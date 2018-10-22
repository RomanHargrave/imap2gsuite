#!/usr/bin/env python2.7
from common import Connection
import common

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

# upload to group
def upload_folder(log, service, args, folder):
    archive = service.archive()

    failure_count   = 0
    success_count   = 0
    total_count     = len(folder.mailpieces)
    print_countdown = args.print_after

    log.info('Uploading folder "%s", with %d mailpieces' % (folder.name, total_count))

    for message in folder.mailpieces:
        try:
            strio = io.BytesIO(message.r822)
            media = apiclient.http.MediaIoBaseUpload(strio, mimetype='message/rfc822')
            time.sleep(1.0 / args.rate_limit)
            result = archive.insert(groupId=args.group, media_body=media).execute()
            assert result['responseCode'].lower() == 'success'
            log.debug('Uploaded mailpieces #%d' % (message.id))

        except Exception as ex:
            log.exception('Failed to upload message with IMAP ID %d in folder "%s"; subject "%s"' % (message.id, folder.name, message.subject))
            failure_count += 1

        print_countdown -= 1
        if print_countdown == 0:
            log.info("... uploaded %d of %d mailpieces in %s (errors = %d)" % (success_count, total_count, folder.name, failure_count))
            print_countdown = args.print_after

    log.info('... folder "%s" completed: out of %d messages, %d succeeded and %d failed' % (folder.name, total_count, success_count, failure_count))

def main():
    parser = argparse.ArgumentParser(parents = [oauth2client.tools.argparser], description = 'Migrate an IMAP account to Google Groups')
    parser.add_argument('server',           help='IMAP Server')
    parser.add_argument('username',         help='IMAP Username')
    parser.add_argument('password',         help='IMAP Password')
    parser.add_argument('group',            help='Google Group Address')
    parser.add_argument('--verbose',        dest='verbose', action='store_true', help='Detailed (read: trace level) logging')
    parser.add_argument('--ssl',            dest='use_ssl', action='store_true', help='Use SSL (Requires Valid Cert)')
    parser.add_argument('--report',         dest='print_after', type=int, default=100, help='Print status every N mailpieces (per folder)')
    parser.add_argument('--rate',           dest='rate_limit', type=int, default=10, help='API Rate Limit')
    parser.add_argument('--cid',            dest='client_secret_file', default='client_secret.json')
    parser.add_argument('--cred-store',     dest='credential_file', default=os.path.join(os.path.expanduser('~'), '.google', 'imap2group.json'))

    logging.basicConfig(format = '[%(asctime)s %(name)s %(levelname)s] %(message)s')

    log  = logging.getLogger('imap2group')
    args = parser.parse_args()

    if (args.verbose):
        log.setLevel(logging.DEBUG)


    connection  = Connection(args.server, args.username, args.password, ssl=args.use_ssl)
    service     = connect_service(args)

    for folder in connection.folders:
        upload_folder(log, service, args, folder)

if __name__ == "__main__":
    main()

