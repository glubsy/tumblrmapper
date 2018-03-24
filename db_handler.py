#!/bin/env python
import os
import sys
import fdb
import time
import re
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


def create_blank_db_file(username, userpassword, dbpath=None):
    """creates the db at host"""
    if dbpath is None: #FIXME: default file in script dir (permissions issues)
        dbpath = os.path.dirname(__file__) + "/" + "blank_db.fdb"
    # ("create database 'host:/temp/db.db' user 'sysdba' password 'pass'")
    c = r"create database " + r"'" + dbpath + r"' user '" + username + r"' password '" + userpassword + r"'"
    fdb.create_database(c)

def populate_db_with_tables(username, userpassword, dbpath=None):
    """Create our tables and procedures here in the DB"""
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    with fdb.TransactionContext(con): #auto rollback if exception is raised, and no need to close() because automatic
        # cur = con.cursor()
        # Create domains
        con.execute_immediate("CREATE DOMAIN LONG_TEXT AS VARCHAR(500);")
        con.execute_immediate("CREATE DOMAIN SUPER_LONG_TEXT AS VARCHAR(32765)")
        con.execute_immediate("CREATE DOMAIN BOOLEAN AS smallint \
                               CHECK (VALUE IS NULL OR VALUE IN (0, 1));")

        # Create tables with columns
        con.execute_immediate("CREATE TABLE BLOGS ( AUTO_ID SMALLINT PRIMARY KEY, \
                                                    BLOG_NAME varchar(60) NOT NULL,\
                                                    HEALTH varchar(5),\
                                                    TOTAL_POSTS INTEGER,\
                                                    STATUS varchar(10),\
                                                    OFFSET INTEGER,\
                                                    LAST_CHECKED TIMESTAMP, \
                                                    CONSTRAINT blogs_unique UNIQUE (BLOG_NAME) using index ix_blogs \
                                                    );")
                                                    #FIXME: AUTO_ID not needed?

        con.execute_immediate("CREATE TABLE GATHERED_BLOGS ( \
                               AUTO_ID SMALLINT PRIMARY KEY, \
                               BLOG_NAME varchar(60) UNIQUE \
                               );")
                               # make constraint / trigger, to only create if not already in tBLOGS?
    
        con.execute_immediate("CREATE TABLE POSTS ( \
                                POST_ID BIGINT PRIMARY KEY, \
                                REMOTE_ID BIGINT, \
                                BLOG_ORIGIN SMALLINT NOT NULL, \
                                BLOG_REBLOGGED SMALLINT, \
                                POST_URL varchar(500) NOT NULL, \
                                POST_DATE varchar(26), \
                                FOREIGN KEY(BLOG_ORIGIN) REFERENCES BLOGS(AUTO_ID), \
                                FOREIGN KEY(BLOG_REBLOGGED) REFERENCES GATHERED_BLOGS(AUTO_ID) \
                                );")

        con.execute_immediate("CREATE TABLE REBLOGGED_POSTS ( \
                               REMOTE_ID BIGINT PRIMARY KEY, \
                               BLOG_NAME SMALLINT, \
                               POST_ID BIGINT, \
                               POST_URL varchar(500), \
                               FOREIGN KEY(BLOG_NAME) REFERENCES GATHERED_BLOGS(AUTO_ID), \
                               FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID) \
                               );")
                               # REMOTE_ID is the post id in the trail, in case of reblog for example
                               # URL might need to be parsed from Caption

        con.execute_immediate("CREATE TABLE CONTEXTS ( \
                               AUTO_ID BIGINT PRIMARY KEY, \
                               POST_ID BIGINT NOT NULL, \
                               REMOTE_ID BIGINT, \
                               CONTEXT LONG_TEXT UNIQUE, \
                               FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID), \
                               FOREIGN KEY(REMOTE_ID) REFERENCES REBLOGGED_POSTS(REMOTE_ID) \
                               );") 
                               #FIXME: maybe not unique context for performance?

        con.execute_immediate("CREATE TABLE URLS ( \
                               FILE_URL varchar(500) PRIMARY KEY, \
                               POST_ID BIGINT NOT NULL, \
                               FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID) \
                               ); ")


        con.execute_immediate("CREATE TABLE OLD_1280 ( FILENAME varchar(60), FILEBASENAME varchar(60) )")

        # Create triggers
        con.execute_immediate("CREATE SEQUENCE tBLOGS_id_sequence;")
        con.execute_immediate("CREATE SEQUENCE tCONTEXTS_id_sequence;")
        con.execute_immediate("CREATE SEQUENCE tGATHERED_BLOGS_id_sequence;")

        con.execute_immediate("CREATE TRIGGER tBLOGS_AUTOINC FOR BLOGS \
                                ACTIVE BEFORE INSERT POSITION 0 \
                                AS BEGIN NEW.AUTO_ID = next value for tBLOGS_id_sequence; END")
    
        con.execute_immediate("CREATE TRIGGER tCONTEXTS_AUTOINC FOR CONTEXTS \
                                ACTIVE BEFORE INSERT POSITION 0 \
                                AS BEGIN NEW.AUTO_ID = next value for tCONTEXTS_id_sequence; END")

        con.execute_immediate("CREATE TRIGGER tGATHERED_BLOGS_AUTOINC FOR GATHERED_BLOGS \
                                ACTIVE BEFORE INSERT POSITION 0 \
                                AS BEGIN NEW.AUTO_ID = next value for tGATHERED_BLOGS_id_sequence; END")

        # Create procedures
        # con.execute_immediate("CREATE PROCEDURE insert_post ")
        # con.execute_immediate("CREATE PROCEDURE check_blog_status")

def populate_db_with_archives(archivelist_path, username, userpassword, dbpath=None):
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


def populate_db_with_blogs(blogslist_path, username, userpassword, dbpath=None):
    """read archive list and populate the OLD_1280 table"""
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    data = readlines(blogslist_path)
    t0 = time.time()
    with fdb.TransactionContext(con):
        for line in data.splitlines():
            try:
                sql = "INSERT INTO BLOGS (ID) VALUES ('" + line + "')"
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


def Create_blank_database(dbpath):
    # test typical usage:
    create_blank_db_file("sysdba", "masterkey", dbpath)
    populate_db_with_tables("sysdba", "masterkey", dbpath)
    # populate_db_with_archives(scriptdir + "tools/1280_files_list.txt", "sysdba", "masterkey", dbpath)
    # the_db = Database(db_host="localhost", db_filepath=dbpath, db_user="test", db_password="test")

def temp_update_table(dbpath):
    con = fdb.connect(database=dbpath, user=username, password=userpassword)
    with fdb.TransactionContext(con):
        con.execute_immediate('')


if __name__ == "__main__":
    Create_blank_database("/home/nupupun/test/tumblrgator_test.fdb")
    # populate_db_with_blogs(scriptdir + "tools/blogs_toscrape.txt", "sysdba", "masterkey", "/home/nupupun/test/tumblrgator_test.fdb")
