#!/usr/bin/python3
#
# Build the file list of archived files for which we need to recover their _raw
#
# Generates archives_files_list_minus_todelete.txt which is a result of:
#   archives_1280_tuple.txt
#   minus -> raw_downloads_tuple.txt (or raw_archives_autogen.txt if not present)
#   minus -> archive_files_to_delete (if exists)
#
import os
import pickle
import re
import sys
from collections import Counter
import db_handler
import fdb


tumblr_base_noext_re = re.compile(r'(tumblr_(?:inline_|messaging_)?.*?(?:_r\d)?)_.{3,4}.*') # tumblr_xxx_r1
# tumblr_base_norev_noext_re = re.compile(r'(tumblr_.*)(?:_r\d)?_.{3,4}.*')
# exclude revision
tumblr_base_norev_noext_re = re.compile(r'(tumblr_(?:inline_|messaging_)?.*?)(?:_r\d)?_.{3,4}.*')

SCRIPTDIR = os.path.dirname(__file__)
DEBUG = True
to_delete_archives_set_debug_path = SCRIPTDIR + os.sep + "tools/to_delete_archives_set_debug.txt" #debug



def get_raw_files_from_db(database):
    """Returns tuple of all _raw files found"""
    con = database.connect()
    cur = con.cursor()

    cur.execute(r"""select * from FILES where (FILE_NAME SIMILAR TO 'tumblr\_%\_raw.%' escape '\');""")
    cur.execute(
r"""execute BLOCK
returns (v_f varchar(500), v_fp varchar(500))
as
declare variable v_p bigint;
BEGIN
for select FILE_NAME, PATH_ID from FILES where (FILE_NAME SIMILAR TO 'tumblr\_%\_raw.%' escape '\') into :v_f, v_p
do
begin
    execute procedure SP_GET_FULL_PATH(v_p, '/') returning_values :v_fp;
    suspend;
END
END""")
    # return {k:v for k,v in cur.fetchall()} # dictionaries where key is filename, dirpath is value
    return tuple(cur.fetchall()) #tuple of lists


def get_1280_from_db(database):
    """Returns a dict of all _1280, _500, _250 files but not _raw found in DB,
    key is filename, value is filepath"""

    con = database.connect()
    cur = con.cursor()
    cur.execute(
r"""execute BLOCK
returns (v_f varchar(500), v_fp varchar(500))
as
declare variable v_p bigint;
BEGIN
for select FILE_NAME, PATH_ID from FILES where (FILE_NAME SIMILAR TO 'tumblr\_%\_[0-9]{3,4}.%' escape '\')
and (FILE_NAME NOT SIMILAR TO 'tumblr\_%\_raw.%' escape '\') into :v_f, v_p
do
begin
    execute procedure SP_GET_FULL_PATH(v_p, '/') returning_values :v_fp;
    suspend;
END
END""")
    # return {k:v for k,v in cur.fetchall()} # dictionaries where key is filename, dirpath is value
    return tuple(cur.fetchall()) #tuple of lists


def separate_lists(tuple_result):
    """Transforms ([filename, filepath], [filename2, filepath2], [filename2, filepath2])
    into tuple of two lists of ([filenames], [filepaths])"""
    return tuple(map(list, zip(*tuple_result)))


def merge_lists(list_result):
    """Transforms [[filenames], [filepaths]] back into
    ([filename, filepath], [filename2, filepath2], [filename2, filepath2])"""
    return tuple(map(list, zip(list_result[0], list_result[1])))


def remove_raw_alternatives_from_1280(list_1280, list_raw, keep_rev=False, slow=False):
    """Returns a set of difference"""
    if not isinstance(tuple(), type(list_1280)):
        if isinstance(str(), type(list_1280)) and not "cache" in list_1280:
            _list_1280 = readfile(list_1280, evaluate=True)
        else:   # is string path to pickled tuple of two lists
            _list_1280 = readfile_pickle(list_1280)
            if len(_list_1280) != 2:
                _list_1280 = separate_lists(_list_1280)
    else:       # must be a tuple()
        _list_1280 = list_1280
    # write_to_file(SCRIPTDIR + os.sep + "tools/_list_1280", _list_1280)

    if not isinstance(tuple(), type(list_raw)):
        if isinstance(str(), type(list_raw)) and not "cache" in list_raw:
            _raw_set = readfile(list_raw, evaluate=True)
        else:   # is string path to pickled tuple of two lists
            _raw_set = separate_lists(readfile_pickle(list_raw))
            if len(_raw_set) != 2:
                _raw_set = separate_lists(_raw_set)
    else:       # must be a tuple()
        _raw_set = list_raw

    # below, assuming the two input sets have the exact same file format!
    if slow: # FIXME untested! not really needed and HORRIBLE CODE (mutating while iterating)
        for item in _list_1280:
            match = tumblr_base_noext_re.match(item)
            if match:
                reresult = match.group(1)
                for item in _raw_set:
                    if item == reresult:
                        print("found {0} to delete, removing {1}"
                        .format(item, reresult))
                        _list_1280.remove(item)
        return _list_1280

    else:
        trimmed_list_1280 = remove_file_string_extensions(_list_1280[0], keep_rev=keep_rev)

        trimmed_list_raw = remove_file_string_extensions(_raw_set[0], keep_rev=keep_rev)

        # _list_1280 minus common items with _raw_set
        # return filtered_list_1280.difference(filetered_list_raw)

        # list of files, list of their dirs, list of _raw files
        return difference(trimmed_list_1280, _list_1280[1], trimmed_list_raw)



def difference(trimmed_list_1280, trimmed_paths_1280, trimmed_list_raw):
    """remove dupes at index, for both _list[0] and _list[1]"""
    filtered_files = list()
    filtered_paths = list()
    index = -1
    for trimmed_file in trimmed_list_1280:
        index += 1
        if trimmed_file in trimmed_list_raw:
            continue
        else:
            filtered_files.append(trimmed_file)
            filtered_paths.append(trimmed_paths_1280[index])

    return (filtered_files, filtered_paths)


def remove_file_string_extensions(myset, keep_rev=False):
    newset = list()
    # count = 0
    for item in myset:
        if keep_rev:
            match = tumblr_base_noext_re.search(item)
        else:
            match = tumblr_base_norev_noext_re.search(item)
        if match:
            # count += 1
            newset.append(match.group(1))

    # print("count: " + str(count))

    # counter = Counter(newset)
    # print(counter.most_common(50))

    return newset


def remove_todelete_from_list(current_1280_set, todelete_txt, keep_rev=False, slow=False, debug=False):
    """Returns current_set minus the files found in todelete_txt
    if slow, without extension"""

    # below, assuming the two input sets have the exact same file format!
    if slow: # FIXME untested! not really needed and HORRIBLE CODE (mutating while iterating)
        todelete_set = readfile(todelete_txt, evaluate=False)

        for item in todelete_set:
            match = tumblr_base_noext_re.match(item)
            if match:
                reresult = match.group(1)
                for item in current_1280_set:
                    if item == reresult:
                        print("Found {0} to delete, removing {1}"
                        .format(item, reresult))
                        current_1280_set.remove(item)
        return current_1280_set

    else:
        todelete_tuple = remove_file_string_extensions(readfile(todelete_txt, evaluate=False), keep_rev=keep_rev)
        todelete_set = set(todelete_tuple)
        if DEBUG:
            print("Length of generated todelete_set: {0}".format(len(todelete_set)))
            write_to_file(to_delete_archives_set_debug_path, todelete_set)

        print("doing difference between current_set: {0} and todelete_set: {1}"
        .format(len(current_1280_set[0]), len(todelete_set)))

        return difference(current_1280_set[0], current_1280_set[1], todelete_set)



def readfile_pickle(filepath):
    """Returns the object written as a pickle in a file"""

    with open (filepath, 'rb') as fp:
        fileset = pickle.load(fp)

    return fileset


def readfile(filepath, evaluate=False):
    """Returns a set of each line in file"""

    listoflists = list()

    with open(filepath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            if evaluate:
                listoflists.append(eval(line))
            else:
                listoflists.append(line)

    return tuple(listoflists)


def write_to_file(filepath, mylist, use_pickle=False):
    if use_pickle:
        if not isinstance(tuple(), type(mylist)):
            with open(filepath, 'wb') as fp:
                pickle.dump(mylist, fp)
        else:
            with open(filepath, 'wb') as fp:
                # _list = tuple(map(list, zip(mylist[0], mylist[1]))) #FIXME
                _list = mylist
                pickle.dump(_list, fp)
    else:
        if not isinstance(tuple(), type(mylist)):
            with open(filepath, 'w') as f:
                for item in mylist:
                    f.write("{}\n".format(item))
        else: # isinstance(set(), type(mylist))
            with open(filepath, 'w') as f:
                # for item in tuple(map(list, zip(mylist[0], mylist[1]))):
                for item in mylist:
                    f.write("{}\n".format(item))



def main(output_pickle=True, keep_revision=True):

    # FIXME: lots of hard coded values could be in config
    # _1280 files listing
    archives_1280_txt_path = SCRIPTDIR + os.sep + "tools/archives_1280_list.txt"
    archives_1280_txt_cache = SCRIPTDIR + os.sep + "tools/archives_1280_cache.txt" # caching
    archives_1280_tuple_debug = SCRIPTDIR + os.sep + "tools/archives_1280_tuple_debug.txt" # debug

    # _raw files listing
    raw_downloads_txt_path = SCRIPTDIR + os.sep + "tools/downloads_raw_list.txt"
    raw_downloads_txt_cache = SCRIPTDIR + os.sep + "tools/downloads_raw_cache.txt" #caching
    raw_downloads_tuple_debug = SCRIPTDIR + os.sep + "tools/downloads_raw_set_debug.txt" #debug

    # optional todelete files (filter out)
    to_delete_archives_txt = os.path.expanduser("~/Documents/DOCUMENTS/archive_files_to_delete.txt")

    # final filtered file outputs
    archives_minus_raw_path = SCRIPTDIR + os.sep + "tools/archives_minus_raw.txt"
    archives_minus_raw_minus_todelete_path = SCRIPTDIR + os.sep + "tools/archives_minus_raw_minus_todelete.txt"
    archives_minus_raw_minus_todelete_path_pickle = SCRIPTDIR + os.sep + "tools/archives_minus_raw_minus_todelete_pickle.txt"

    # archives _1280
    CGI_database = db_handler.Database(db_filepath="/home/firebird/CGI.vvv", \
                        username="sysdba", password="masterkey")

    bluray_database = db_handler.Database(db_filepath="/home/firebird/bluray_backups.vvv", \
                        username="sysdba", password="masterkey")

    # current downloads _raw
    downloads_database = db_handler.Database(db_filepath="/home/firebird/downloads.vvv", \
                        username="sysdba", password="masterkey")

    customTPB = fdb.TPB()
    customTPB.access_mode = fdb.isc_tpb_read  # read only

    archives_1280_tuple = None
    raw_downloads_tuple = None
    raw_downloads_tuple_sep = None

    if not os.path.exists(archives_1280_txt_path):
        if not os.path.exists(archives_1280_txt_cache):
            archives_1280_tuple_cgi     = get_1280_from_db(CGI_database)
            archives_1280_tuple_bluray  = get_1280_from_db(bluray_database)
            archives_1280_tuple_merged  = archives_1280_tuple_cgi + archives_1280_tuple_bluray

            print("Before separation, length of generated archives_1280_tuple_merged: {}"
            .format(len(archives_1280_tuple_merged)))
            # write_to_file(SCRIPTDIR + os.sep + "tools/pre_sep.txt", archives_1280_tuple_merged, use_pickle=False)

            archives_1280_tuple = separate_lists(archives_1280_tuple_merged)

            print("Length of generated archives_1280_tuple: {}".format(len(archives_1280_tuple[0])))
            write_to_file(archives_1280_txt_cache, archives_1280_tuple, use_pickle=True)
            if DEBUG:
                write_to_file(archives_1280_tuple_debug, archives_1280_tuple, use_pickle=False)

    # fetch _raw files from DB if text file doesn't already exist
    if not os.path.exists(raw_downloads_txt_path):
        if not os.path.exists(raw_downloads_txt_cache):
            raw_downloads_tuple     = get_raw_files_from_db(downloads_database)
            raw_downloads_tuple_sep = separate_lists(raw_downloads_tuple)

            print("Length of generated raw_downloads_tuple: {}".format(len(raw_downloads_tuple_sep[0])))

            write_to_file(raw_downloads_txt_cache, raw_downloads_tuple_sep, use_pickle=True)

            if DEBUG:
                write_to_file(raw_downloads_tuple_debug, raw_downloads_tuple_sep, use_pickle=False)


    if raw_downloads_tuple_sep is not None:
        if archives_1280_tuple is not None:
            archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_tuple, raw_downloads_tuple_sep, keep_revision)
        else: # use path
            if os.path.exists(archives_1280_txt_path):
                archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_txt_path, raw_downloads_tuple_sep, keep_revision)
            elif os.path.exists(archives_1280_txt_cache):
                archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_txt_cache, raw_downloads_tuple_sep, keep_revision)
            else:
                print("ERROR: no 1280 listing found!")
                sys.exit(0)

    else: # we have raw_downloads_txt_path
        if archives_1280_tuple is not None:
            if os.path.exists(raw_downloads_txt_path):
                archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_tuple, raw_downloads_txt_path, keep_revision)
                print(archives1280_minus_raw_tuple)
            elif os.path.exists(raw_downloads_txt_cache):
                archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_tuple, raw_downloads_txt_cache, keep_revision)
            else:
                print("ERROR: no raw listing found!")
        else: # we have no 1280 set nor raw set
            if os.path.exists(archives_1280_txt_path) and os.path.exists(raw_downloads_txt_path):
                archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_txt_path, raw_downloads_txt_path, keep_revision)
            elif os.path.exists(archives_1280_txt_cache) and os.path.exists(raw_downloads_txt_path):
                archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_txt_cache, raw_downloads_txt_path, keep_revision)
            elif os.path.exists(archives_1280_txt_cache) and os.path.exists(raw_downloads_txt_cache):
                archives1280_minus_raw_tuple = remove_raw_alternatives_from_1280(archives_1280_txt_cache, raw_downloads_txt_cache, keep_revision)
            else:
                print("ERROR: no 1280 listing nor raw listing found!")

    print("archives_minus_raw_tuple length: {0}.".format(len(archives1280_minus_raw_tuple[0])))
    write_to_file(archives_minus_raw_path, archives1280_minus_raw_tuple, use_pickle=False)


    # archives1280_final = archives1280_minus_raw - to_delete_archives_txt
    if os.path.exists(to_delete_archives_txt):

        archives_minus_raw_minus_todelete_tuple_sep = remove_todelete_from_list(archives1280_minus_raw_tuple,
        to_delete_archives_txt, debug=DEBUG)

        archives_minus_raw_minus_todelete_tuple = merge_lists(archives_minus_raw_minus_todelete_tuple_sep)

        print("archives_minus_raw_minus_todelete_tuple length {0}."
        .format(len(archives_minus_raw_minus_todelete_tuple)))

        # final_archives_minus_raw_minus_todelete = tuple(map(list, zip(archives_minus_raw_minus_todelete_tuple[0], archives_minus_raw_minus_todelete_tuple[1])))

        if output_pickle:
            write_to_file(archives_minus_raw_minus_todelete_path_pickle, archives_minus_raw_minus_todelete_tuple, use_pickle=True)
        else:
            write_to_file(archives_minus_raw_minus_todelete_path, archives_minus_raw_minus_todelete_tuple)
        if DEBUG:
            write_to_file(archives_minus_raw_minus_todelete_path, archives_minus_raw_minus_todelete_tuple, use_pickle=False)



if __name__ == "__main__":
    DEBUG = True
    main(output_pickle=True)
