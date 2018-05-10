#!/usr/bin/env python3.6

# Reads a csv file and downloads the items

import os
import sys
import requests
import csv
import fdb
import shutil
import traceback
import subprocess
import signal
import re
from tqdm import tqdm
from constants import BColors
SCRIPTDIR = os.path.abspath(os.path.dirname(__file__))
DATAPATH = '/data1TB/CGI_fix/redownloads'
regex = re.compile(r"(.*?)\d{1,2}\.media(\.tumblr\.com.*?tumblr_.*)_\d{3,4}(\..*)", re.I)
sigint_again = False
global asked_termination
asked_termination = False

def main(args):
    successfull_downloads_file = SCRIPTDIR + os.sep + 'tools/successful_downloads.txt'
    failed_downloads_file = SCRIPTDIR + os.sep + 'tools/failed_downloads.txt'

    request_session = requests.Session()
    request_session.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; \
rv:55.0) Gecko/20100101 Firefox/55.0'})
    

    for filebasename, url in read_csv(args[0]):

        signal.signal(signal.SIGINT, signal_handler)

        global asked_termination
        if asked_termination:
            break

        #fetch in DB for dir -> then download
        url = re.sub(regex, r'\1data\2_raw\3', url)
        url = url.replace('https://', 'http://')

        # get the directories in DB associated with each archive
        for item in fetch_archive_in_db(filebasename, args[1]):
            # print("from db: {}".format(item))
            dir_list = item[2].split('##')
            for directory in dir_list:
                make_dir(DATAPATH, directory)
            try:
                download_file(url, dir_list, request_session)
            except BaseException as e:
                # traceback.print_exc()
                print("{}Error while downloading file {}: {}{}"
                .format(BColors.FAIL, url.split('/')[-1], e, BColors.ENDC))
                write_downloaded_to_log(failed_downloads_file, filebasename, url)
                remove_first_line(args[0])
                continue

            write_downloaded_to_log(successfull_downloads_file, filebasename, url)
            remove_first_line(args[0])



def write_downloaded_to_log(writepath, filebasename, url):
    with open(writepath, 'a') as f:
        f.write('\t'.join((filebasename, url)) + '\n')


def download_file(url, filepath_list, request_session):

    destfilename = url.split('/')[-1]
    tqdm.write("GET {} into {}".format(url, [i for i in filepath_list]))

    for dirpath in filepath_list:
        if os.path.exists(DATAPATH + os.sep + dirpath + os.sep + destfilename):
            print("{}File {} already existed. url was {}{}"
            .format(BColors.RED, destfilename, url, BColors.ENDC))
            return

    try:
        req = request_session.get(url, stream=True)
    except BaseException as e:
        traceback.print_exc()
        print('{0}Error while downloading {1}: {2}{3}'
        .format(BColors.FAIL, url, e, BColors.ENDC))
        try:
            print("req: {}".format(req.json()))
        except:
            pass
        raise

    if not (200 <= req.status_code <= 400):
        tqdm.write("{0}Server error downloading {1} {2}{3}"
        .format(BColors.FAIL, url, req.status_code, BColors.ENDC))
        raise BaseException("Not found")
    else:
        with open(DATAPATH + os.sep + filepath_list[0] + os.sep + destfilename, 'wb') as file_handler:
            pbar = tqdm(unit="B", total=int(req.headers['Content-Length']))
            tqdm.write("saving {}".format(filepath_list[0] + os.sep + destfilename))

            for chunk in req.iter_content(chunk_size=1024):
                signal.signal(signal.SIGINT, signal_handler)
                if chunk: # filter out keep-alive new chunks
                    pbar.update(len(chunk))
                    file_handler.write(chunk)
            pbar.close()
        tqdm.write("{}Download of {} completed!\n-------------------------"
        .format(BColors.GREENOK, url))

        if len(filepath_list) > 1:
            #copy file into other dirs
            for filepath in filepath_list[1:]:
                if not os.path.exists(DATAPATH + os.sep + filepath + os.sep + destfilename):
                    print("{}copying {} as a duplicate into {}{}"
                    .format(BColors.MAGENTA, destfilename, filepath, BColors.ENDC))

                    shutil.copy(DATAPATH + os.sep + filepath_list[0] + os.sep + destfilename,
                    DATAPATH + os.sep + filepath + os.sep + destfilename)
                else:
                    print("{}file {} already exists in {}, skipping copying{}"
                    .format(BColors.RED, destfilename, filepath, BColors.ENDC))


def make_dir(datapath, mypath):
    """Create physical directory in advance, where we will download the file
    datapath is root dir DATAPATH, mypath is the rest of the tree"""
    if not os.path.exists(datapath + os.sep + mypath):
        os.makedirs(datapath + os.sep + mypath)
        pass


def fetch_archive_in_db(filebasename, db):
    """Returns a tuple of rows for each archive record found"""
    con = fdb.connect(database=db['filepath'],
                        user=db['username'], password=db['password'])
    cur = con.cursor()
    cur.execute(r"""SELECT * FROM OLD_1280 where filebasename = '""" + filebasename + r"""';""")
    return cur.fetchall()


def read_csv(filepath):
    f = open(filepath, 'r')
    # lines = f.readlines()[1:5] # only the first 5 lines
    lines = f.readlines()
    mycsv = csv.reader(lines, delimiter='\t')
    for row in mycsv:
        filebasename, url = row
        yield filebasename, url
    f.close()


def remove_first_line(filepath):
    """Remove the first line from file."""
    cmd = ['sed', '-i', '-e', '1d', filepath]
    subprocess_call = subprocess.Popen(cmd, shell=False,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    _, err = subprocess_call.communicate()
    if err:
        raise NameError(
            '\n============= WARNING/ERROR ===============\n{}\n\
===========================================\n'.format(err.rstrip()))


def is_sigint_called_twice(sigint_again):
    """Check if pressing ctrl+c a second time to terminate immediately"""
    if not sigint_again:
        sigint_again = True
        return False
    print("Script has been forcefully terminated")
    return True


def signal_handler(sig, frame):
    """Handles SIGINT signal, blocks it to terminate gracefully
    after the current download has finished"""
    # print('You pressed Ctrl+C!:', signal, frame)
    if is_sigint_called_twice(sigint_again):
        print("\nTerminating script!")
        sys.exit(0)

    global asked_termination
    asked_termination = True
    print(BColors.BLUEOK + "User asked for soft termination, pausing soon." + BColors.ENDC)



if __name__ == '__main__':
    
    myargs = []

    myargs.append(SCRIPTDIR + os.sep + 'tools/found_raw_for_1280_brute.txt')

    database = {'filepath': "/home/firebird/tumblrmapper.fdb",
                        'username': "sysdba", 'password': "masterkey"}
    myargs.append(database)

    main(myargs)
