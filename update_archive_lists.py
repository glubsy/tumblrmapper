#!/usr/bin/python3
#
# Build the file list of archived files for which we need to recover their _raw 
#
# Generates archives_files_list_minus_todelete.txt which is a result of:
#   archives_1280_set.txt
#   minus -> raw_downloads_set.txt (or raw_archives_autogen.txt if not present)
#   minus -> archive_files_to_delete (if exists)
#

import tumblrmapper
import db_handler
import sys
import os
import pickle
from collections import Counter
import fdb
import re

# tumblr_base_noext_re = re.compile(r'(tumblr_.*)_.{3,4}.*') # tumblr_xxx_r1
# tumblr_base_norev_noext_re = re.compile(r'(tumblr_.*)(?:_r\d)?_.{3,4}.*')
# exclude revision 
tumblr_base_norev_noext_nongreedy_re = re.compile(r'(tumblr_(?:inline_)?.*?)(?:_r\d)?_.{3,4}.*')


def get_raw_files_from_db(database):
    """Returns tuple of all _raw files found"""
    con = database.connect()
    cur = con.cursor()

    cur.execute(r"""select * from FILES where (FILE_NAME SIMILAR TO 'tumblr\_%\_raw.%' escape '\');""")
    results = set()
    for item in cur.fetchall():
        results.add(item[1])
    print("Got {0} _raw items from DB".format(len(results)))
    return results


def get_1280_from_db(database):
    """Returns tuple of all _1280, _500, _250 files but not _raw found in DB"""

    con = database.connect()
    cur = con.cursor()
    cur.execute(
r"""
select * from FILES where (FILE_NAME SIMILAR TO 'tumblr\_%\_[0-9]{3,4}.%' escape '\')
and (FILE_NAME NOT SIMILAR TO 'tumblr\_%\_raw.%' escape '\');
""")
    results = set()
    for item in cur.fetchall():
        results.add(item[1])
    print("Got {0} 1280 items from DB".format(len(results)))
    return results


def remove_raw_alternatives_from_1280(list_1280, list_raw, slow=False):
    """Returns a set of difference"""
    if not isinstance(set, type(list_1280)):
        if not "autogen" in list_1280 and isinstance(str, type(list_1280)):
            _list_1280 = readfile(list_1280)
        else:   # is string path to pickle
            _list_1280 = readfile_pickle(list_1280)
    else:       # must be a set()
        _list_1280 = list_1280

    if not isinstance(set, type(list_raw)):
        if not "autogen" in list_raw and isinstance(str, type(list_raw)):
            _raw_set = readfile(list_raw)
        else:   # is string path to pickle
            _raw_set = readfile_pickle(list_raw)
    else:       # must be a set()
        _raw_set = list_raw

    if slow:
        for item in _list_1280:
            match = tumblr_base_norev_noext_nongreedy_re.match(item)
            if match:
                reresult = match.group(1)

                for item in _raw_set:
                    if item == reresult:
                        print("found {0} to delete, removing {1}"
                        .format(item, reresult))
                        _list_1280.remove(item)
        return _list_1280

    else:
        filtered_list_1280 = remove_file_string_extensions(_list_1280)

        filetered_list_raw = remove_file_string_extensions(_raw_set)

        # _list_1280 minus common items with _raw_set
        return filtered_list_1280.difference(filetered_list_raw)


def remove_file_string_extensions(myset):
    newset = set()
    # count = 0
    for item in myset:
        match = tumblr_base_norev_noext_nongreedy_re.search(item)
        if match:
            # count += 1
            newset.add(match.group(1))

    # print("count: " + str(count))

    # counter = Counter(newset)
    # print(counter.most_common(50))

    return newset


def remove_todelete_from_list(current_1280_set, todelete_txt, slow=False):
    """Returns current_set minus the files found in todelete_txt
    if slow, without extension"""

    if slow:
        todelete_set = readfile(todelete_txt)

        for item in todelete_set:
            match = tumblr_base_norev_noext_re.match(item)
            if match:
                reresult = match.group(1)
                for item in current_1280_set:
                    if item == reresult:
                        print("found {0} to delete, removing {1}"
                        .format(item, reresult))
                        current_1280_set.remove(item)

    else:
        todelete_set = remove_file_string_extensions(readfile(todelete_txt))
        # print("length of todelete set: {0}".format(len(todelete_set)))

        # print("doing difference between current_set: {0} and todelete_set: {1}"
        # .format(len(current_1280_set), len(todelete_set)))
        return current_1280_set.difference(todelete_txt)
    


def readfile_pickle(filepath):
    """Returns a set of each line in file"""

    with open (filepath, 'rb') as fp:
        fileset = pickle.load(fp)

    return fileset


def readfile(filepath):
    """Returns a set of each line in file"""

    fileset = set()
    
    with open(filepath, 'r') as f:
        for line in f:
            fileset.add(line)

    return fileset


def write_to_file(filepath, mylist, use_pickle=False):
    if use_pickle:
        with open(filepath, 'wb') as fp:
            pickle.dump(mylist, fp)
    else:
        with open(filepath, 'w') as f:
            for item in mylist:
                f.write("{}\n".format(item))





if __name__ == "__main__":
    SCRIPTDIR = os.path.dirname(__file__)

    # _1280 files listing
    archives_1280_txt_path = SCRIPTDIR + os.sep + "tools/archives_files_list.txt"
    archives_1280_txt_autogen = SCRIPTDIR + os.sep + "tools/archives_files_list_autogen.txt" #caching

    # _raw files listing
    raw_downloads_txt_path = SCRIPTDIR + os.sep + "tools/raw_archives.txt"
    raw_downloads_txt_autogen = SCRIPTDIR + os.sep + "tools/raw_archives_autogen.txt" #caching

    # optional todelete files (filter out)
    to_delete_archives_txt = os.path.expanduser("~/Documents/DOCUMENTS/archive_files_to_delete.txt")
    
    # final filtered file output
    archives_files_list_minus_todelete_path = SCRIPTDIR + os.sep + "tools/archives_files_list_minus_todelete.txt"


    # current downloads _raw
    downloads_database = db_handler.Database(db_filepath="/home/firebird/downloads.vvv", \
                        username="sysdba", password="masterkey")

    # archives _1280
    CGI_database = db_handler.Database(db_filepath="/home/firebird/CGI.vvv", \
                        username="sysdba", password="masterkey")

    archives_1280_set = None
    raw_downloads_set = None

    if not os.path.exists(archives_1280_txt_path):
        if not os.path.exists(archives_1280_txt_autogen):
            archives_1280_set = get_1280_from_db(CGI_database)
            print("length of archives_1280_set: {}".format(len(archives_1280_set)))
            write_to_file(archives_1280_txt_autogen, archives_1280_set, use_pickle=True)
            # DEBUG
            # write_to_file(SCRIPTDIR + os.sep + "tools/archives_1280_set.txt", archives_1280_set, use_pickle=False)

    # fetch _raw files from DB if text file doesn't already exist
    if not os.path.exists(raw_downloads_txt_path):
        if not os.path.exists(raw_downloads_txt_autogen):
            raw_downloads_set = get_raw_files_from_db(downloads_database)
            print("length of raw_downloads_set: {}".format(len(raw_downloads_set)))
            write_to_file(raw_downloads_txt_autogen, raw_downloads_set, use_pickle=True)
            # DEBUG
            # write_to_file(SCRIPTDIR + os.sep + "tools/raw_downloads_set.txt", raw_downloads_set, use_pickle=False)


    if raw_downloads_set is not None:
        if archives_1280_set is not None:
            archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_set, raw_downloads_set)
        else: # use path
            if os.path.exists(archives_1280_txt_path):
                archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_txt_path, raw_downloads_set)
            elif os.path.exists(archives_1280_txt_autogen):
                archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_txt_autogen, raw_downloads_set)
            else:
                print("ERROR: no 1280 listing found!")
                sys.exit(0)

    else: # we have raw_downloads_txt_path
        if archives_1280_set is not None:
            if os.path.exists(raw_downloads_txt_path):
                archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_set, raw_downloads_txt_path)
                print(archives1280_minus_raw_set)
            elif os.path.exists(raw_downloads_txt_autogen):
                archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_set, raw_downloads_txt_autogen)
            else:
                print("ERROR: no raw listing found!")
        else: # we have no 1280 set nor raw set
            if os.path.exists(archives_1280_txt_path) and os.path.exists(raw_downloads_txt_path):
                archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_txt_path, raw_downloads_txt_path)
            elif os.path.exists(archives_1280_txt_autogen) and os.path.exists(raw_downloads_txt_path):
                archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_txt_autogen, raw_downloads_txt_path)
            elif os.path.exists(archives_1280_txt_autogen) and os.path.exists(raw_downloads_txt_autogen):
                archives1280_minus_raw_set = remove_raw_alternatives_from_1280(archives_1280_txt_autogen, raw_downloads_txt_autogen)
            else:
                print("ERROR: no 1280 listing nor raw listing found!")

    print("archives_minus_raw_set: {0}".format(len(archives1280_minus_raw_set)))
    # DEBUG
    write_to_file(SCRIPTDIR + os.sep + "tools/archives_minus_raw_set.txt", archives1280_minus_raw_set, use_pickle=False)


    # archives1280_final = archives1280_minus_raw - to_delete_archives_txt
    if os.path.exists(to_delete_archives_txt):
        archives_files_list_minus_todelete = remove_todelete_from_list(archives1280_minus_raw_set, to_delete_archives_txt)
        print("archives_files_list_minus_todelete: {0}".format(len(archives_files_list_minus_todelete)))
        write_to_file(archives_files_list_minus_todelete_path, archives_files_list_minus_todelete)
