#!/bin/env python
import os
import sys
import fdb
import time
import re
import json
from pprint import pprint
from constants import BColors
import traceback
# import logging
scriptdir = os.path.dirname(__file__) + "/"

class Database():
    """handle the db file itself, creating everything"""
    
    def __init__(self, db_host, db_filepath, db_user, db_password):
        self.db_host = db_host
        self.db_filepath = db_filepath
        self.username = db_user
        self.userpassword = db_password

    def query_blog(self, queryobj):
        """query DB for blog status"""

    def populate_db_with_procedures(self):
        pass


class Connection(Database):
    """Keeps connection to databases, passes requests"""

    def __init__(self, Database):
        self.con = None #our connection
    
    def connect_to(self, object):
        """initialize connection to remote DB"""
        self.con = fdb.connect(database=str(self.db_host + self.db_filepath), user=self.username, password=self.userpassword)
        return self.con

    def close_connection(self, con):
        """close con"""
        self.con.close()


def create_blank_db_file(dbpath, username, userpassword):
    """creates the db at host"""
    if dbpath is None: #FIXME: default file in script dir (permissions issues)
        dbpath = os.path.dirname(__file__) + "/" + "blank_db.fdb"
    # ("create database 'host:/temp/db.db' user 'sysdba' password 'pass'")
    c = r"create database " + r"'" + dbpath + r"' user '" + username + r"' password '" + userpassword + r"'"
    fdb.create_database(c)

def populate_db_with_tables(dbpath, username, userpassword):
    """Create our tables and procedures here in the DB"""
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    with fdb.TransactionContext(con): #auto rollback if exception is raised, and no need to close() because automatic
        # cur = con.cursor()
        # Create domains
        con.execute_immediate("CREATE DOMAIN LONG_TEXT AS VARCHAR(500);")
        con.execute_immediate("CREATE DOMAIN D_BLOG_NAME AS VARCHAR(60);")
        con.execute_immediate("CREATE DOMAIN D_POST_NO AS BIGINT;")
        con.execute_immediate("CREATE DOMAIN SUPER_LONG_TEXT AS VARCHAR(32765)")
        con.execute_immediate("CREATE DOMAIN BOOLEAN AS smallint \
                               CHECK (VALUE IS NULL OR VALUE IN (0, 1));")

        # Create tables with columns
        con.execute_immediate("CREATE TABLE BLOGS ( BLOG_NAME       D_BLOG_NAME PRIMARY KEY ,\
                                                    HEALTH          varchar(5),\
                                                    TOTAL_POSTS     INTEGER,\
                                                    STATUS          varchar(10) DEFAULT 'new',\
                                                    OFFSET          INTEGER,\
                                                    LAST_CHECKED    TIMESTAMP \
                                                    );")

        con.execute_immediate("CREATE TABLE GATHERED_BLOGS ( \
                               BLOG_NAME    D_BLOG_NAME PRIMARY KEY );")
                               #TODO: make constraint CHECK to only create if not already in tBLOGS?
    
        con.execute_immediate("CREATE TABLE POSTS ( \
                                POST_ID             D_POST_NO PRIMARY KEY, \
                                REMOTE_ID           D_POST_NO, \
                                ORIGIN_BLOGNAME     D_BLOG_NAME NOT NULL, \
                                REBLOGGED_BLOGNAME  D_BLOG_NAME, \
                                POST_URL            LONG_TEXT NOT NULL, \
                                POST_DATE           varchar(26), \
                                FOREIGN KEY(ORIGIN_BLOGNAME) REFERENCES BLOGS(BLOG_NAME), \
                                FOREIGN KEY(REBLOGGED_BLOGNAME) REFERENCES GATHERED_BLOGS(BLOG_NAME) \
                                );")
                                #BLOG_ORIGIN is the blog of the post_id (TID)
                                #BLOG_REBLOGGED is name of blog in trail

        con.execute_immediate("CREATE TABLE REBLOGGED_POSTS ( \
                               REMOTE_ID    D_POST_NO PRIMARY KEY, \
                               BLOG_NAME    D_BLOG_NAME, \
                               POST_ID      D_POST_NO, \
                               POST_URL     LONG_TEXT, \
                               FOREIGN KEY(BLOG_NAME) REFERENCES GATHERED_BLOGS(BLOG_NAME), \
                               FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID) \
                               );")
                               # MEMO: this is to keep track of which bloggers reblogged that post
                               # REMOTE_ID is the post id in the trail, in case of reblog for example
                               # POST_URL might need to be parsed from Caption

        con.execute_immediate("CREATE TABLE CONTEXTS ( \
                               AUTO_ID      BIGINT PRIMARY KEY, \
                               POST_ID      D_POST_NO NOT NULL, \
                               REMOTE_ID    D_POST_NO, \
                               SLUG         VARCHAR(200),\
                               CONTEXT      LONG_TEXT, \
                               FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID), \
                               FOREIGN KEY(REMOTE_ID) REFERENCES REBLOGGED_POSTS(REMOTE_ID) \
                               );")

        con.execute_immediate("CREATE TABLE URLS ( \
                               FILE_URL             LONG_TEXT PRIMARY KEY, \
                               POST_ID              D_POST_NO NOT NULL, \
                               FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID) \
                               ); ")
                                #

        con.execute_immediate("CREATE TABLE OLD_1280 ( FILENAME varchar(60), \
                            FILEBASENAME varchar(60) );")

        # Create triggers
        con.execute_immediate("CREATE SEQUENCE tCONTEXTS_id_sequence;")
    
        con.execute_immediate("CREATE TRIGGER tCONTEXTS_AUTOINC FOR CONTEXTS \
                                ACTIVE BEFORE INSERT POSITION 0 \
                                AS BEGIN NEW.AUTO_ID = next value for tCONTEXTS_id_sequence; END")

        # Create procedures
        con.execute_immediate("CREATE OR ALTER PROCEDURE insert_post \
                                (   i_postid d_post_no, \
                                    i_blog_origin d_blog_name,\
                                    i_post_url varchar(500),\
                                    i_post_date varchar(26),\
                                    i_remoteid d_post_no default null,\
                                    i_blogname_reblogged d_blog_name default null\
                                )\
                                AS declare variable v_fetched_blogname_reblogged d_blog_name default null;\
                                declare variable v_b_update_gathered boolean default 0;\
                                BEGIN\
                                if (:i_blogname_reblogged is not null) THEN \
                                select BLOG_NAME from GATHERED_BLOGS where BLOG_NAME = :i_blogname_reblogged into :v_fetched_blogname_reblogged; \
                                if ((:i_blogname_reblogged is distinct from :i_blog_origin) and (:v_fetched_blogname_reblogged is null)) \
                                THEN v_b_update_gathered = 1;\
                                if (v_b_update_gathered = 1) THEN\
                                INSERT into GATHERED_BLOGS (BLOG_NAME) values (:i_blogname_reblogged);\
                                INSERT into POSTS (POST_ID, POST_URL, POST_DATE, REMOTE_ID, \
                                ORIGIN_BLOGNAME, REBLOGGED_BLOGNAME)\
                                values (:i_postid, :i_post_url, :i_post_date, :i_remoteid,\
                                :i_blog_origin, :i_blogname_reblogged);\
                                END"
                                )

        # con.execute_immediate("CREATE or ALTER PROCEDURE insert_context \
        # con.execute_immediate("CREATE or ALTER PROCEDURE insert_url \
        # con.execute_immediate("CREATE or ALTER PROCEDURE insert_url \


        # con.execute_immediate("CREATE PROCEDURE check_blog_status")


        # Create views
        # con.execute_immediate("CREATE VIEW v_posts ( \
        #                         POST_ID, REMOTE_ID, BLOG_ORIGIN, BLOG_REBLOGGED, POST_URL, POST_DATE) \
        #                         AS SELECT \
        #                         POST_ID, REMOTE_ID, BLOG_ORIGIN, BLOG_REBLOGGED, POST_URL, POST_DATE, AUTO_ID, BLOG_NAME \
        #                         FROM POSTS, GATHERED_BLOGS, BLOGS \
        #                         WHERE POSTS.BLOG_ORIGIN = BLOGS.AUTO_ID, POSTS.BLOG_REBLOGGED = GATHERED_BLOGS.BLOG_NAME \
        #                         );")


def populate_db_with_archives(dbpath, username, userpassword, archivelist_path):
    """read archive list and populate the OLD_1280 table"""
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    cur = con.cursor()
    oldfiles = readlines(archivelist_path)
    repattern_tumblr = re.compile(r'(tumblr_.*)_.*\..*', re.I) #eliminate '_size.ext'
    repattern_revisions = re.compile(r'(tumblr_.*)(?:_r\d)', re.I) #elimitane '_r\d'
    t0 = time.time()
    with fdb.TransactionContext(con):
        argsseq = list()
        for line in oldfiles.splitlines():
            reresult = repattern_tumblr.search(line) 
            basefilename = reresult.group(1) #tumblr_azec_azceniaoiz1_r1
            reresult2 = repattern_revisions.search(basefilename)
            if reresult2:
                basefilename = reresult2.group(1) #tumblr_azec_azceniaoiz1
            argsseq.append((line, basefilename))

        sql = cur.prep("INSERT INTO OLD_1280 (FILENAME, FILEBASENAME) VALUES (?, ?)")
        cur.executemany(sql, argsseq)
        con.commit()
    t1 = time.time()
    print('Inserting records into OLD_1280 Took %.2f ms' % (1000*(t1-t0)))


def populate_db_with_blogs(dbpath, username, userpassword, blogslist_path):
    """read archive list and populate the OLD_1280 table"""
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    data = readlines(blogslist_path)
    t0 = time.time()
    with fdb.TransactionContext(con):
        for line in data.splitlines():
            try:
                sql = "INSERT INTO BLOGS (BLOG_NAME) VALUES ('" + line + "')"
                con.execute_immediate(sql)
            except fdb.fbcore.DatabaseError as e:
                if "violation of PRIMARY or UNIQUE KEY" in e.__str__():
                    print("Error when inserting blog: " + line + " is already recorded.")
    t1 = time.time()
    print('Inserting records into BLOGS Took %.2f ms' % (1000*(t1-t0)))


def readlines(filepath):
    """read a newline separated file list"""

    with open(filepath, 'r') as f:
        data = f.read()
    return data


def Create_blank_database(dbpath, user, userpassword):
    # test typical usage:
    scriptpath = os.path.dirname(__file__) + os.sep
    blogs_toscrape = scriptpath + "tools/blogs_toscrape.txt"
    archives_toload = scriptpath +  "tools/1280_files_list.txt"

    # create_blank_db_file(dbpath, user, userpassword)
    # populate_db_with_tables(dbpath, user, userpassword)
    # populate_db_with_blogs(dbpath, user, userpassword, blogs_toscrape)
    # populate_db_with_archives(dbpath, user, userpassword, archives_toload)
    
    # the_db = Database(db_host="localhost", db_filepath=dbpath, db_user="test", db_password="test")
    
    test_update_table(dbpath, user, userpassword)

class UpdatePayload(dict):
    pass

def test_update_table(dbpath, username, userpassword):
    """feed testing data"""
    update = parse_json_response(json.load(open\
    ("/home/nupupun/Programming/tumblrgator/tools/test/videogame-fantasy_july_reblogfalse.json", 'r'))['response'])
    t0 = time.time()
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    cur = con.cursor()

    with fdb.TransactionContext(con):
        for post in update.trimmed_posts_list:
            try:
                #TODO: add more arguments to the procedure, make more stored procedures
                operation = cur.prep("execute procedure insert_post(?,?,?,?,?,?)")
                params = (post['id'], post['blog_name'], post['post_url'], post['date'], post['remote_id'], post['reblogged_blog_name'])
                cur.execute(operation, params)
            except Exception as e:
                print(BColors.FAIL + "ERROR in loop:" + str(e) + BColors.ENDC)
                break
        else:
            print("NO BREAK OCCURED IN FOR LOOP")
            con.commit()
    t1 = time.time()
    print('Procedure insert_post took %.2f ms' % (1000*(t1-t0)))

def parse_json_response(json): #TODO: move to client module when done testing
    """returns a UpdatePayload() object that holds the fields to update in DB"""

    update = UpdatePayload()
    update.blogname = json['blog']['name']
    update.totalposts = json['blog']['total_posts']
    update.posts_response = json['posts'] #list of dicts
    update.trimmed_posts_list = [] #list of dicts of posts

    for post in update.posts_response: #dict in list
        current_post_dict = {}
        current_post_dict['id'] = (post['id'])
        current_post_dict['date'] = post['date']
        current_post_dict['post_url'] = post['post_url']
        current_post_dict['slug'] = post['slug']
        current_post_dict['blog_name'] = post['blog_name']
        if 'trail' in post.keys() and len(post['trail']) > 0: # trail is not empty, it's a reblog
            #FIXME: put this in a trail subdictionary
            current_post_dict['reblogged_blog_name'] = post['trail'][0]['blog']['name']
            current_post_dict['remote_id'] = int(post['trail'][0]['post']['id'])
            current_post_dict['remote_content'] = post['trail'][0]['content_raw']
        else: #trail is an empty list
            current_post_dict['reblogged_blog_name'] = None
            current_post_dict['remote_id'] = None
            current_post_dict['remote_content'] = None
            pass

        if 'photos' in post.keys():
            current_post_dict['photos'] = list()
            for item in range(0, len(post['photos'])):
                current_post_dict['photos'].append(post['photos'][item]['original_size']['url'])

        update.trimmed_posts_list.append(current_post_dict)

    for post in update.trimmed_posts_list:
        print("===============================\n\
POST number: " + str(update.trimmed_posts_list.index(post)))
        for key, value in post.items():
            print("key: " + str(key) + "\nvalue: " + str(value) + "\n--")

    return update


if __name__ == "__main__":
    Create_blank_database("/home/nupupun/test/tumblrgator_test.fdb", "sysdba", "masterkey")
    # populate_db_with_blogs(scriptdir + "tools/blogs_toscrape.txt", "sysdba", "masterkey", "/home/nupupun/test/tumblrgator_test.fdb")
