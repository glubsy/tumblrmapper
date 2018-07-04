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

tumblr_base_noext_re = re.compile(r'(tumblr_(?:inline_|messaging_)?.*?(?:_r\d)?)_\d{3,4}.*') # tumblr_xxx_r1_1280

def parse_args():
    """Parse command-line arguments."""

    parser = argparse.ArgumentParser(description='Replace files from archives.')
    parser.add_argument('-i', '--input_dir', action="store", type=str,
    help="Input directory to scan")
    parser.add_argument('-o', '--output_dir', action="store", type=str, default=os.getcwd(),
    help="Directory where files will be moved to temporarily for double check.")

    parser.add_argument('-s', '--ref_dir', action="store", type=str, default="/test/CGI",
    help="Directory where _raw files are stored. Can be set as same as input_dir.")
    
    parser.add_argument('-d', '--delete_mode', action="store_true", default=False,
    help="Move out files found in to_delete_list")
    parser.add_argument('-r', '--replace_files', action="store", default=True,
    help="Move _raw files in place of _1280")


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



def walk_directory(_dir, filter=None):
    """Returns list of tuples [(file's dir path, file's parent dir name, filename)]"""
    filelist = []
    for root, dirs, files in os.walk(_dir):
        for _file in files:
            filelist.append((root, os.path.basename(root), _file))
    return filelist


def main():
    args = parse_args()

    input_dir = args.input_dir
    if args.output_dir is not None:
        output_dir = args.output_dir
    
    if args.to_delete_list is not None:
        to_delete_list =  read_deletion_list(args.to_delete_list)
        # print(f"to_delete_list:\n{to_delete_list}")
        print(f"to_delete_list length: {len(to_delete_list)}")

    if os.path.isdir(args.ref_dir):
        raw_file_list = walk_directory(args.ref_dir, filter="raw")


    subdir_list = []
    for root, dirs, files in os.walk(input_dir):
        for directory in dirs:
            subdir_list.append(os.path.join(root, directory))
    print(f"subdir_list is {subdir_list}")

    for subdir in subdir_list:
        subdir_files = walk_directory(subdir)
        print(f"subdir_files: {subdir_files}")

        for triple in subdir_files:
            # test if to_delete
            if to_delete_list is not None:
                if triple[2].split('.')[0] in to_delete_list:
                    print(f"File {triple[2]} is marked for deletion.")
                    if args.delete_mode:
                        print(f"Moving file to {args.output_dir}")


            # test if tumblr 1280
            match = tumblr_base_noext_re.match(triple[2])
            if match:
                print(f"found {triple[2]} as match")




if __name__ == "__main__":
    main()
