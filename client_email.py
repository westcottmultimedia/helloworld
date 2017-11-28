#
#
# v3. Added Python 3 support. Adjusted for SSL and GMail login.
#   -Adjusted for gmail from http://stackabuse.com/how-to-send-emails-with-gmail-using-python/
# v2. From http://deepinthecode.com/2012/12/05/sending-an-email-with-attachments-using-python-archiving-functionality-added/
#   -added directory searching functionality to add all files in folder
#   -disabled username / password logon for use in our Exchange environment
# v1. Adapted from https://gist.github.com/4009671 and other sources by David Young
#

######### Setup your stuff here #######################################

path='./deliverables/client' # location of files
archiveFolderName = 'archive' # name of folder under path where files will be archived
host = 'smtp.gmail.com' # specify port, if required, using a colon and port number following the hostname

fromaddr = 'eric.chen0121@gmail.com' # must be a vaild 'from' address in your environment
toaddr  = ['eric.chen0121@gmail.com', 'kwestcottg@gmail.com'] # list of email addresses
replyto = fromaddr # unless you want a different reply-to

SERVER_DEBUG_LEVEL = False # set to True for verbose output

# username = 'username' # not used in our Exchange environment
# password = 'password' # not used in our Exchange environment

gmail_user = 'eric.chen0121@gmail.com'
gmail_password = 'Ch3n3r1cEricchen' # <<CHANGE THIS>>

msgsubject = 'Daily feeds for streaming and sales'

text = '''Daily feeds for Buzz Angle Media

    Legend:
    first three numbers in filename:
    001: track streaming
    002: iTunes track  sales
    003: iTunes album sales
    004: iTunes music video sales

    last three numbers:
    001: Spotify
    002: Apple

    Example:
    bam_2017-11-25_001_001.csv : track streaming on Spotify
    bam_2017-11-25_003_002.csv : iTunes album sales on Apple
'''
# htmlmsgtext = '''<h2>Daily feeds</h2>''' # text with appropriate HTML tags

######### In normal use nothing changes below this line ###############

import smtplib, os, sys, shutil
from datetime import date
import email
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders
from html.parser import HTMLParser

archivePath = os.path.join(path, archiveFolderName) # full path where files will be archived

if not os.path.exists(archivePath): # create archive folder if it doesn't exist
    os.makedirs(archivePath)
    print('Archive folder created at ' + archivePath + '.')

# A snippet - class to strip HTML tags for the text version of the email

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()
#
# ########################################################################
#
try:

    # # necessary mimey stuff
    msg = MIMEMultipart()
    msg.preamble = 'This is a multi-part message in MIME format.n'
    msg.epilogue = ''

    # Make text version from HTML - First convert tags that produce a line break to carriage returns
    # msgtext = htmlmsgtext.replace('</br>','r').replace('<br />','r').replace('</p>','r')
    # Then strip all the other tags out
    # msgtext = strip_tags(msgtext)

    body = MIMEMultipart('alternative')
    # body.attach(MIMEText(msgtext))
    # body.attach(MIMEText(htmlmsgtext, 'html'))
    body.attach(MIMEText(text))
    msg.attach(body)
    attachments = os.listdir(path)
    if '.DS_Store' in attachments: attachments.remove('.DS_Store')

    print('ATTACHMENTS:', attachments)
    if 'attachments' in globals() and len('attachments') > 0: # are there attachments?
        for filename in attachments:
            if os.path.isfile(os.path.join(path, filename)): # remove any folders!
                f = os.path.join(path, filename)
                part = MIMEBase('application', 'octet-stream')
                part.set_payload( open(f,'rb').read() )
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename={}'.format(os.path.basename(f)))
                msg.attach(part)

        msg['From'] = fromaddr
        msg['To'] = COMMASPACE.join(toaddr)
        msg['Subject'] = msgsubject
        msg['Reply-To'] = replyto

        print('Email being sent to:')
        print(toaddr)

        # The actual email sendy bits
        # server = smtplib.SMTP(host, 587) # gmail insecure connection

        server = smtplib.SMTP_SSL(host) # secure connection, recommended!
        server.ehlo()
        server.login(gmail_user, gmail_password)
        server.set_debuglevel(SERVER_DEBUG_LEVEL)

#         Comment this block and uncomment the below try/except block if TLS or user/pass is required.
        server.sendmail(fromaddr, toaddr, msg.as_string())
        print('Email sent.')
        server.quit() # bye bye

    # try:
    #     # If TLS is used
    #     server.starttls()
    #     server.login(username,password)
    #     server.sendmail(msg['From'], [msg['To']], msg.as_string())
    #     print('Email sent.'
    #     server.quit() # bye bye
    # except:
    # # if tls is set for non-tls servers you would have raised an exception, so....
    # server.login(username,password)
    # server.sendmail(msg['From'], [msg['To']], msg.as_string())
    # print('Email sent.'
    # server.quit() # bye bye


    # Archive files to folder
    #
    try:
        if 'attachments' in globals() and len('attachments') > 0: # are there attachments?
            for filename in attachments:
                if os.path.isfile(os.path.join(path, filename)):
                    f1 = os.path.join(path, filename)
                    x = filename.find('.')
                    new_filename = filename[:x] + '_' + 'archived' + filename[x:]
                    f2 = os.path.join(path, new_filename)
                    os.rename(f1, f2)
                    print('-' * 40)
                    print('File ' + filename + ' renamed to ' + new_filename + '.')
                    shutil.move(f2, archivePath)
                    print('File ' + new_filename + ' moved to ' + archivePath + '.')

    except:
        print('Files not successfully renamed and/or archived.')

except:
    print('Email NOT sent to %s successfully. ERR: %s %s %s ' % (str(toaddr), str(sys.exc_info()[0]), str(sys.exc_info()[1]), str(sys.exc_info()[2]) ) )
