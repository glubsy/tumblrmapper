#!/usr/bin/env python3.6
# scan op_dir,
# for each _1280 look for a _raw in raw_dir (can be same as op_dir)
# take into acount delete_file_from_archives.txt

import shutil
import os
import sys
import argparse
import re
import shutil
import pickle
from constants import BColors

tumblr_base_noext_re = re.compile(r'(tumblr_(?:inline_|messaging_)?.*?(?:_r\d)?)_(\d{3,4})\..*') # tumblr_xxx_r1_1280
tumblr_base_noext_raw_re = re.compile(r'(tumblr_(?:inline_|messaging_)?.*?(?:_r\d)?)_raw\..*') # tumblr_xxx_r1_raw

def parse_args():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description='Replace files from archives.')
    parser.add_argument('-i', '--input_dir', action="store", type=str, default=os.getcwd(),
    help="Input directory to scan")
    parser.add_argument('-o', '--output_dir', action="store", type=str, default=os.path.expanduser("~/test/CGI_output"),
    help="Directory where files will be moved to temporarily for double check.")

    parser.add_argument('-s', '--ref_dir', action="store", type=str, default=os.path.expanduser("~/test/CGI"),
    help="Directory where _raw files are stored. Can be set as same as input_dir.")

    parser.add_argument('-d', '--delete_mode', action="store_true", default=False,
    help="Move out files found in to_delete_list")
    parser.add_argument('-r', '--replace_files', action="store_false", default=True,
    help="Move _raw files in place of _1280")
    parser.add_argument('-m', '--move_raw_temp_dir', action="store_true", default=False,
    help="Move _raw files into a temporary directory under their original parent dir to keep track of them.\
The symlink alongside the corresponding _1280 will be made against the moved files. Useful to avoid duplicate backups.")
    parser.add_argument('-u', '--update_raw_xattr', action="store", default=False,
    help="Update extended file attributes for _raw files wherever their original urls appears in lists.\
The list should be named 'successful_downloads.txt' and the path should point to a directory holding them.")


    parser.add_argument('-t', '--to_delete_list', action="store",
    default="/home/nupupun/Documents/DOCUMENTS/tumblr_archives/archive_files_to_delete.txt",
    help="path to the list of files to delete.")

    return parser.parse_args()


def read_deletion_list(filepath):
    """build a list from deletion list"""
    _set = set()
    with open(filepath, "r") as f:
        for line in f:
            if line.startswith('#'):
                continue
            line = line.rstrip()
            if line == '':
                continue
            # if line in _set:
            #     print(f"duplicate line in {filepath}: {line}")
            _set.add(line)
    return _set


def write_pickle(thelist, filepath):
    """Write object to file on disk (caching)"""
    with open(filepath, 'wb') as fp:
        pickle.dump(thelist, fp)


def read_pickle(filepath):
    """Returns the object written as a pickle in a file"""
    with open (filepath, 'rb') as fp:
        data = pickle.load(fp)
    return data


def find(name, path):
    """Returns the first occurence of name in path"""
    print(f"find({name})")
    for root, _, files in os.walk(path):
        for _file in files:
            _file = _file.split('.')[0]
            if name in _file:
                return os.path.join(root, name)


def get_relative_path(root, path):
    """Return paths between root and path, returns root if same directory"""
    _ret = os.path.relpath(root, path)
    if _ret == '.':
        return root
    return _ret


def get_list_of_raw_url(path):
    """Make a set of url from the successful raw downloads"""
    if not os.path.isdir:
        print(f"{BColors.FAIL}Path {path} is not a valid directory! Aborting.{BColors.ENDC}")
        return
    _set = set()
    for root, _, files in os.walk(path):
        for _file in files:
            if "successful_downloads" in _file:
                with open(os.path.abspath(os.path.join(root, _file)), 'r') as f:
                    for line in f:
                        _set.add((line.split('\t')[0], line.split('\t')[1]))
    return _set


def had_url(basename):
    """If the first item in each tuple has the same basename as basename, return the
    second item with its original url"""
    global list_of_raw_urls
    for item in list_of_raw_urls:
        if item[0] == basename:
            return item[1]


def apply_xattr(path_to_file, url):
    existing_attr = os.listxattr(path_to_file)
    if not existing_attr:
        print(f"{BColors.LIGHTGRAY}Setting {url} as origin.url for \
{path_to_file}{BColors.ENDC}")
        os.setxattr(path_to_file, "user.xdg.origin.url", url)


def generate_raw_list(path, xattr=False):
    """Returns list of tuples
    [(file's dir path, file's parent dir name, subdirs from root, filename, basename), ...]
    where basename is either the file with no ext, or tumblr_xxxx"""
    filelist = []
    os.chdir(path)
    for root, _, files in os.walk(path):
        for _file in files:
            # print(f"{BColors.BOLD}DEBUG: root: {root} path: {path}{BColors.ENDC}")
            subdirs = get_relative_path(root, path) # dirs between root and file
            # print(f"{BColors.BOLD}DEBUG: subdirs: {subdirs}{BColors.ENDC}")
            match = tumblr_base_noext_raw_re.match(_file)
            if match:
                if xattr:
                    origin_url = had_url(match.group(1))
                    if origin_url:
                        print(f"{BColors.CYAN}File {_file} had url {origin_url}.\
    Applying as xattr.{BColors.ENDC}")
                        apply_xattr(os.path.join(root, _file), origin_url)
                filelist.append((root, os.path.basename(root), subdirs ,_file, match.group(1)))
    return filelist


def main():
    args = parse_args()
    input_dir = args.input_dir

    if args.output_dir is not None:
        output_dir = args.output_dir
        if output_dir == input_dir:
            print(f"{BColors.FAIL}Error: output directory cannot be the same as input directory!")
            exit(1)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

    # cache for _raw file list on disk
    raw_cache_path = "/tmp/" + "tumblrmapper_raw_file_cache_pickle"

    # make temp trash dir to move to_delete_files in it
    trash_dir = output_dir + os.sep + "TRASH_TO_DELETE"
    os.makedirs(trash_dir, exist_ok=True)

    if args.to_delete_list is not None:
        to_delete_list =  read_deletion_list(args.to_delete_list)
        # print(f"to_delete_list:\n{to_delete_list}")
        print(f"{BColors.BOLD}{BColors.BLUE} Number of unique file names to be delete: \
{len(to_delete_list)}{BColors.ENDC}")

    if args.update_raw_xattr:
        global list_of_raw_urls
        # list_of_raw_urls = get_list_of_raw_url(os.path.join(os.getcwd(), "tools"))
        list_of_raw_urls = get_list_of_raw_url(args.update_raw_xattr)

    # Set up list of the _raw files we want to compare against
    if os.path.isdir(args.ref_dir):
        if not os.path.isfile(raw_cache_path):
            if args.update_raw_xattr and list_of_raw_urls:
                xattr = True
            else
                xattr = False
            raw_file_cache = generate_raw_list(args.ref_dir, xattr=xattr)
            write_pickle(raw_file_cache, raw_cache_path)
        else:
            raw_file_cache = read_pickle(raw_cache_path)
    print(f"raw_file_cache: {raw_file_cache}")

    # Make list of directories to scan for 1280 files
    for root, _, files in os.walk(input_dir):
        for _file in files:
            subdirs = get_relative_path(root, input_dir) # dirs between root and file
            old_file_tuple = (root, os.path.basename(root), subdirs ,_file, _file.split('.')[0])
            print(f"Processing file: {old_file_tuple}")

            # test if file is present in the to_delete list
            if args.delete_mode and to_delete_list is not None:
                if old_file_tuple[4] in to_delete_list:
                    # print(f"File {old_file_tuple[2]} is marked for deletion.")
                    print(f"{BColors.LIGHTYELLOW}Moving {old_file_tuple[3]} \
to output_dir:{BColors.ENDC} {trash_dir}")
                    sub_trash_dir = trash_dir + os.sep + old_file_tuple[2]
                    os.makedirs(name=sub_trash_dir, exist_ok=True)
                    try:
                        #FIXME DEBUG, should be move here
                        shutil.copy(os.path.join(old_file_tuple[0], old_file_tuple[3]),
                                os.path.join(sub_trash_dir, old_file_tuple[3]))
                        # shutil.move(os.path.join(old_file_tuple[0], old_file_tuple[3]),
                        #         os.path.join(sub_trash_dir, old_file_tuple[3]))
                        print(f"{BColors.RED}Moved file to be deleted \
{old_file_tuple[3]} to {sub_trash_dir}{BColors.ENDC}")
                    except FileExistsError as e:
                        print(f"{BColors.FAIL}Error moving file \
{os.path.join(old_file_tuple[0], old_file_tuple[3])}:{BColors.ENDC} {e}\n")
                    continue

            # test if tumblr 1280/500/250
            match = tumblr_base_noext_re.match(old_file_tuple[3])
            if match:
                print(f"Found resized: {old_file_tuple[3]}")
                # Look for corresponding _raw in _raw file list,
                # Removing extension, and resized suffix from name
                base_name = old_file_tuple[4]
                # Eliminate whatever is after _1280
                base_name = re.split("_" + match.group(2), base_name)[0]
                print(f"Looking for corresponding _raw in cache: {base_name}")
                corresponding_raw_index = find_in_cache(base_name, raw_file_cache)

                if corresponding_raw_index is not None:
                    print(f"{BColors.GREENOK}Found {raw_file_cache[corresponding_raw_index]} \
corresponding to {old_file_tuple[3]}{BColors.ENDC}")

                    replace_by_raw(old_file_tuple, raw_file_cache[corresponding_raw_index], output_dir, args)

    count_number_of_unique_files(trash_dir)


def replace_by_raw(old_file_tuple, raw_file_tuple, output_dir, args):
    """Replace the old file with the raw file, make symlink in the trash dir"""

    # copy old _1280 into output_dir
    output_subdir = output_dir + os.sep + old_file_tuple[2]
    print(f"output_sudbir = {output_subdir}")
    os.makedirs(name=output_subdir, exist_ok=True)

    # copy2 preserve metadata whenever possible
    shutil.copy2(old_file_tuple[0] + os.sep + old_file_tuple[3],
        output_subdir + os.sep + old_file_tuple[3])

    # move _raw into subdir to keep track of them between backups
    if args.move_raw_temp_dir:
        raw_temp_dir = raw_file_tuple[0] + os.sep \
            + "temp_moved_raw"
        os.makedirs(name=raw_temp_dir, exist_ok=True)

        ref_raw_path = shutil.move(os.path.join(raw_file_tuple[0], raw_file_tuple[3]),
            raw_temp_dir + raw_file_tuple[3])

        print(f"{BColors.GREEN}Moved {raw_file_tuple[3]} \
to {ref_raw_path}{BColors.ENDC}")

    else: # not moved
        ref_raw_path = os.path.join(raw_file_tuple[0], raw_file_tuple[3])

    # symlink _raw where old has been moved
    try:
        os.symlink(
            ref_raw_path,
            os.path.join(output_subdir, raw_file_tuple[3]))
    except FileExistsError as e:
        print(f"{BColors.FAIL}Error creating symlink \
{os.path.join(output_subdir, raw_file_tuple[3])}:{BColors.ENDC} {e}\n")


def find_in_cache(name, list_of_tuples):
    """scan the list of tuples for name, returns index of name in list_of_tuples"""

    for item in list_of_tuples:
        if item[4] == name:
            print(f"{BColors.BOLD}DEBUG: Found _raw for {name}: {item[3]}{BColors.ENDC}")
            return list_of_tuples.index(item)


def count_number_of_unique_files(path):
    """Counts the number of unique files in path"""
    _set = set()
    for _, _, files in os.walk(path):
        for _file in files:
            _set.add(_file)
    print(f"{BColors.BOLD}{BColors.BLUE}There are {len(_set)} unique filenames \
in {path}{BColors.ENDC}")


if __name__ == "__main__":
    main()
