#!/usr/bin/python

import tumblrmapper
import db_handler
import sys
import os
import fdb


def get_raw_files_from_db(database):
    con = database.connect()

    con.execute("""select * from FILES where (FILE_NAME SIMILAR TO 'tumblr\_%\_raw.%' escape '\');""")
    return con.fetchall()


def get_1280_from_db(database):
    con = database.connect()

    con.execute(
"""
select * from FILES where (FILE_NAME SIMILAR TO 'tumblr\_%\_[0-9]{3,4}.%' escape '\') 
and (FILE_NAME NOT SIMILAR TO 'tumblr\_%\_raw.%' escape '\')
""")
    return cur.fetchall()


def remove_raw_alternatives_from_1280(txt_1280, txt_raw):
    list_1280 = readfile(txt_1280)
    list_rax = readfile(txt_raw)
    filtered_set = set()
    for item in list_1280:
        if item not in list_raw
            filtered_list.add(item)

    return filtered_set

def readfile(file):
    filelist = set()
    with open(file, 'r') as f:
        for line in f:
            filelist.add(line)
    return filelist

if __name__ == "__main__":
    SCRIPTDIR = os.path.dirname(__file__)
    args = tumblrmapper.parse_args()
    tumblrmapper.configure_logging(args)

    raw_downloads_txt = SCRIPTDIR + os.sep + "tools/raw_archives.txt"
    archives_1280_txt = SCRIPTDIR + +os.sep + "tools/archives_files_list.txt"
    to_delete_archives_txt = "/home/nupupun/Document/DOCUMENTS/archive_files_to_delete.txt"
    blogs_toscrape_txt = "/home/nupupun/Document/DOCUMENTS/tumblr_scrape_todo.txt"


    # current downloads _raw
    downloads_database = db_handler.Database(db_filepath="/home/firebird/downloads.vvv", \
                        username="sysdba", password="masterkey")

    # archives _1280
    CGI_database = db_handler.Database(db_filepath="/home/firebird/CGI.vvv", \
                        username="sysdba", password="masterkey")

    rawlist = get_raw_files_from_db(downloads_database)

    # archives_1280_list = get_1280_from_db(CGI_database)

    archives1280_minus_raw = remove_raw_from_1280(archives_1280_txt, raw_downloads_txt)
    # archives1280_minus_raw = archives_1280_txt - raw_downloads_txt
    # archives1280_final = archives1280_minus_raw - to_delete_archives_txt


    