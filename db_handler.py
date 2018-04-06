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
import csv
import tumblr_client
# import logging
from operator import itemgetter
SCRIPTDIR = os.path.dirname(__file__) + os.sep
# import cProfile

class Database():
    """handle the db file itself, creating everything  
    Args are: filepath, user, password, bloglist, archives, host=None"""
    
    def __init__(self, *args, **kwargs):
        """initialize with environment"""
        self.host = kwargs.get('db_host', 'localhost') #not implemented
        self.db_filepath = kwargs.get('db_filepath', SCRIPTDIR + "blank_db.fdb")
        self.username = kwargs.get('username', "sysdba")
        self.password = kwargs.get('password', "masterkey")
        self.con = None

    def connect(self):
        """initialize connection to remote DB"""
        if self.con is not None:
            print("Already connected to DB.")
            return self.con
        if self.host == 'localhost':
            self.con = fdb.connect(database=self.db_filepath, user=self.username, password=self.password)
        else:
            self.con = fdb.connect(database=str(self.host + ":" + self.db_filepath), user=self.username, password=self.password)
        return self.con

    def close(self):
        return self.con.close()

    def query_blog(self, queryobj):
        """query DB for blog status"""

    def populate_db_with_procedures(self):
        pass


def create_blank_db_file(database):
    """creates the db at host"""

    # ("create database 'host:/temp/db.db' user 'sysdba' password 'pass'")
    c = r"create database " + r"'" + database.db_filepath + r"' user '" + database.username + r"' password '" + database.password + r"'"
    fdb.create_database(c)

def populate_db_with_tables(database):
    """Create our tables and procedures here in the DB"""
    con = fdb.connect(database=database.db_filepath, user=database.username, password=database.password)
    with fdb.TransactionContext(con): #auto rollback if exception is raised, and no need to con.close() because automatic
        # cur = con.cursor()
        # Create domains
        con.execute_immediate("CREATE DOMAIN D_LONG_TEXT AS VARCHAR(500);")
        con.execute_immediate("CREATE DOMAIN D_URL AS VARCHAR(150);")
        con.execute_immediate("CREATE DOMAIN D_POSTURL AS VARCHAR(300);")
        con.execute_immediate("CREATE DOMAIN D_AUTO_ID AS smallint;")
        con.execute_immediate("CREATE DOMAIN D_BLOG_NAME AS VARCHAR(60);")
        con.execute_immediate("CREATE DOMAIN D_EPOCH AS BIGINT;")
        con.execute_immediate("CREATE DOMAIN D_POST_NO AS BIGINT;")
        con.execute_immediate("CREATE DOMAIN D_SUPER_LONG_TEXT AS VARCHAR(32765)")
        con.execute_immediate("CREATE DOMAIN D_BOOLEAN AS smallint default 0 \
                               CHECK (VALUE IS NULL OR VALUE IN (0, 1));")

        # Create tables with columns
        con.execute_immediate(
            "CREATE TABLE BLOGS ( \
            AUTO_ID         D_AUTO_ID PRIMARY KEY,\
            BLOG_NAME       D_BLOG_NAME,\
            HEALTH          varchar(5),\
            TOTAL_POSTS     INTEGER,\
            CRAWL_STATUS    varchar(10) DEFAULT 'new',\
            CRAWLING        D_BOOLEAN default 0,\
            POST_OFFSET     INTEGER,\
            POSTS_SCRAPED   INTEGER,\
            LAST_CHECKED    TIMESTAMP, \
            LAST_UPDATE     D_EPOCH,\
            PRIORITY        smallint,\
            CONSTRAINT blognames_unique UNIQUE (BLOG_NAME) using index ix_blognames\
            );")
            # HEALTH:  
            # CRAWL STATUS

        con.execute_immediate(
            "CREATE TABLE GATHERED_BLOGS (\
            AUTO_ID      D_AUTO_ID PRIMARY KEY, \
            BLOG_NAME    D_BLOG_NAME );")
            #TODO: make constraint CHECK to only create if not already in tBLOGS?
    
        con.execute_immediate(
            "CREATE TABLE POSTS ( \
            POST_ID             D_POST_NO PRIMARY KEY, \
            REMOTE_ID           D_POST_NO, \
            ORIGIN_BLOGNAME     D_AUTO_ID NOT NULL, \
            REBLOGGED_BLOGNAME  D_AUTO_ID, \
            POST_URL            D_POSTURL NOT NULL, \
            POST_DATE           varchar(26), \
            FOREIGN KEY(ORIGIN_BLOGNAME) REFERENCES BLOGS(AUTO_ID), \
            FOREIGN KEY(REBLOGGED_BLOGNAME) REFERENCES GATHERED_BLOGS(AUTO_ID) \
            );")
            #BLOG_ORIGIN is the blog of the post_id (TID)
            #BLOG_REBLOGGED is name of blog in trail

        con.execute_immediate(
            "CREATE TABLE CONTEXTS ( \
            POST_ID         D_POST_NO,\
            REMOTE_ID       D_POST_NO UNIQUE, \
            TTIMESTAMP      D_EPOCH, \
            CONTEXT         D_SUPER_LONG_TEXT, \
            LATEST_REBLOG   D_POST_NO,\
            PRIMARY KEY(POST_ID),\
            FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID) \
            );")
            # to update
            # if remote_id is null, then it's an original post, not reblogged, we store everything no problem
            # if we already have that remote_id, we don't want to store context again -> we skip
            # otherwise only IF the timestamp we hold is newer, then we UPDATE the context
            # LATEST_REBLOG is the latest reblog that we used to update the timestamp and context fields of an original post
            # that we had recorded. 
            # REMOTE_ID can be NULL! (allowed with unique)  
            # if we input an existing REMOTE_ID (with a new POST_ID, because reblogged by many POST_ID), then EXCEPTION!

        con.execute_immediate(
            "CREATE TABLE URLS ( \
            FILE_URL             D_URL PRIMARY KEY, \
            POST_ID              D_POST_NO NOT NULL, \
            REMOTE_ID            D_POST_NO, \
            FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID) \
            ); ")
            # if remote_id is null, it means it was not a reblog
            # if it's not null, it's from a reblog,

        con.execute_immediate(
            "CREATE TABLE OLD_1280 ( FILENAME varchar(60), FILEBASENAME varchar(60) );")

        # CREATE generators and triggers
        con.execute_immediate("CREATE SEQUENCE tBLOGS_autoid_sequence;")
        con.execute_immediate("CREATE SEQUENCE tGATHERED_BLOGS_autoid_sequence;")
        con.execute_immediate(
            "CREATE TRIGGER tGATHERED_BLOGS_AUTOINC FOR GATHERED_BLOGS \
            ACTIVE BEFORE INSERT POSITION 0 \
            AS BEGIN NEW.AUTO_ID = next value for tGATHERED_BLOGS_autoid_sequence; END")

        # CREATE procedures
        # Records given blogname into BLOG table, increments auto_id,
        # decrements auto_id in case an exception occured (on non unique inputs)
        con.execute_immediate(  
            "CREATE OR ALTER PROCEDURE insert_blogname \
            ( i_blogname d_blog_name, i_prio smallint default null ) \
            AS declare variable v_generated_auto_id d_auto_id;\
            BEGIN \
            v_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence, 1);\
            INSERT into BLOGS (AUTO_ID, BLOG_NAME, PRIORITY) values (:v_generated_auto_id, :i_blogname, :i_prio);\
            WHEN ANY \
            DO \
            v_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence, -1);\
            END ")

        # Inserts a post and all its metadata
        con.execute_immediate(  
            "CREATE OR ALTER PROCEDURE insert_post \
            (   i_postid d_post_no, \
                i_blog_origin d_blog_name,\
                i_post_url d_posturl,\
                i_post_date varchar(26),\
                i_remoteid d_post_no default null,\
                i_reblogged_blog_name d_blog_name default null\
            )\
            AS declare variable v_blog_origin_id d_auto_id;\
            declare variable v_fetched_reblogged_blog_id d_auto_id default null;\
            declare variable v_b_update_gathered d_boolean default 0;\
            BEGIN\
            select AUTO_ID from BLOGS where BLOG_NAME = :i_blog_origin into :v_blog_origin_id;\
            \
            if (:i_reblogged_blog_name is not null) THEN \
            select AUTO_ID from GATHERED_BLOGS where BLOG_NAME = :i_reblogged_blog_name into :v_fetched_reblogged_blog_id;\
            \
            if ((:i_reblogged_blog_name is distinct from :i_blog_origin) and (:v_fetched_reblogged_blog_id is null)) \
            THEN v_b_update_gathered = 1;\
            \
            if ((v_b_update_gathered = 1) and (:i_reblogged_blog_name is not null)) THEN\
            INSERT into GATHERED_BLOGS (BLOG_NAME) values (:i_reblogged_blog_name)\
            returning (AUTO_ID) into :v_fetched_reblogged_blog_id;\
            \
            INSERT into POSTS (POST_ID, POST_URL, POST_DATE, REMOTE_ID, \
            ORIGIN_BLOGNAME, REBLOGGED_BLOGNAME)\
            values (:i_postid, :i_post_url, :i_post_date, :i_remoteid, \
            :v_blog_origin_id, :v_fetched_reblogged_blog_id);\
            END"\
            )

        # Inserts context, update if already present with latest reblog's values
        con.execute_immediate("\
            CREATE OR ALTER PROCEDURE insert_context(\
                i_post_id d_post_no not null, \
                i_timestamp integer,\
                i_context d_super_long_text default null,\
                i_remote_id d_post_no default null)\
            as \
            BEGIN\
            if (:i_remote_id is not null) then /* we might not want to keep it*/\
                    if (exists (select (REMOTE_ID) from CONTEXTS where (REMOTE_ID = :i_remote_id))) then\
                    begin\
                        if (:i_timestamp > (select (TTIMESTAMP) from CONTEXTS where (REMOTE_ID = :i_remote_id))) then\
                            update CONTEXTS set CONTEXT = :i_context, \
                            TTIMESTAMP = :i_timestamp,\
                            LATEST_REBLOG = :i_post_id\
                            where (REMOTE_ID = :i_remote_id);\
                            exit;\
                    end\
            else /* we store everything, it's an original post*/\
            insert into CONTEXTS (POST_ID, TTIMESTAMP, CONTEXT, REMOTE_ID ) values\
                                (:i_post_id, :i_timestamp, :i_context, :i_remote_id );\
            END")

        con.execute_immediate("\
            CREATE OR ALTER PROCEDURE FETCH_ONE_BLOGNAME\
            RETURNS (\
                O_NAME D_BLOG_NAME,\
                O_OFFSET INTEGER,\
                O_HEALTH VARCHAR(5),\
                O_STATUS VARCHAR(10),\
                O_TOTAL INTEGER,\
                O_SCRAPED INTEGER,\
                O_CHECKED TIMESTAMP )\
            AS\
            BEGIN\
            if (exists (select (BLOG_NAME) from BLOGS where ((CRAWL_STATUS = 'resume') and (CRAWLING != 1)))) then begin\
                for \
                select BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED\
                from BLOGS where ((CRAWL_STATUS = 'resume') and (CRAWLING != 1)) order by PRIORITY desc nulls last ROWS 1\
                into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked \
                as cursor cur do\
                    update BLOGS set CRAWLING = 1 where current of cur;\
                suspend;\
                end\
            else\
            if (exists (select (BLOG_NAME) from BLOGS where (CRAWL_STATUS = 'new'))) then begin\
                for select BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED\
                     from BLOGS where (CRAWL_STATUS = 'new') \
                    order by PRIORITY desc nulls last ROWS 1 \
                    into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked as cursor tcur do\
                    update BLOGS set CRAWL_STATUS = 'fetched' where current of tcur;\
                suspend;\
                end\
            END")

        # Update info fetched from API
        # args:  (blogname, health(UP,DEAD,WIPED), totalposts, updated_timestamp, status(resume, dead) )
        con.execute_immediate("\
            CREATE OR ALTER PROCEDURE insert_blog_init_info (\
                i_name D_BLOG_NAME,\
                i_health varchar(5),\
                i_total integer, \
                i_updated d_epoch,\
                i_status varchar(10) default 'resume',\
                i_crawling d_boolean default 0)\
                AS declare variable v_checked d_epoch;\
                BEGIN\
                select DATEDIFF(second FROM timestamp '1/1/1970 00:00:00' TO current_timestamp)\
            from rdb$database into :v_checked;\
                update BLOGS set \
                HEALTH = :i_health, \
                TOTAL_POSTS = :i_total, \
                CRAWL_STATUS = :i_status, \
                CRAWLING = :i_crawling, \
                LAST_UPDATE = :i_updated,\
                LAST_CHECKED = :v_checked\
                where BLOG_NAME = :i_name;\
            END")


        # called when quitting script, or done scaping total_posts
        con.execute_immediate("\
            CREATE OR ALTER PROCEDURE update_crawling_blog_status (\
                i_name d_blog_name, i_input d_boolean) AS BEGIN\
                update BLOGS set CRAWLING = 0 where BLOG_NAME = :i_name; END")


        con.execute_immediate("\
            CREATE PROCEDURE reset_all_crawling AS BEGIN\
                update BLOGS set CRAWLING = 0; END")\

            #reset column CRAWLING on script startup in case we halted without cleaning

        # con.execute_immediate(
            # "CREATE PROCEDURE check_blog_status")
            # retrieve health check: 
            # if health is "alive" and status not "crawling"
            # THEN retrieve total_posts check: if null, go http test it and update total_posts, if 0 change health to wiped
            # if health is "dead" THEN return dead
            # if health is wiped, keep wiped (but crawl still)

            # if status is new: not initialized, can start -> fetch_blog_info(blog)
            # if status is DONE: all scraped, skip
            # if status is CRAWLING: skip
            # if status is RESUME: fetch offset
            # if total_post > posts_scraped: start crawling at offset


        # con.execute_immediate(
        # "CREATE PROCEDURE update_blog_status")
            # on table BLOGS:
            # when total_posts = posts_scraped -> set status to "DONE"
            # update timestamp on post insert_post committed
            # update last post done on insert_post committed
            # update offset on each post insert_post committed (last offset done)
                                

        # Create views
        # con.execute_immediate(
        # "CREATE VIEW v_posts ( \
        # POST_ID, REMOTE_ID, BLOG_ORIGIN, BLOG_REBLOGGED, POST_URL, POST_DATE) \
        # AS SELECT \
        # POST_ID, REMOTE_ID, BLOG_ORIGIN, BLOG_REBLOGGED, POST_URL, POST_DATE, AUTO_ID, BLOG_NAME \
        # FROM POSTS, GATHERED_BLOGS, BLOGS \
        # WHERE POSTS.BLOG_ORIGIN = BLOGS.AUTO_ID, POSTS.BLOG_REBLOGGED = GATHERED_BLOGS.BLOG_NAME \
        # );")


def update_blog_info(database, update):
    """updates blog name in blogs table with values from dictionary"""
    # if dictionary.getitems('health') is OK
    #     stmt = cur.prep("execute procedure update_blog_status(?)")
    #     con.execute(stmt, )
    print(BColors.BLUE + "updating Blogs table with info:" + update + BColors.ENDC)
    cur = database.con.cursor()
    # args: (blogname, UP|DEAD|WIPED, total_posts, updated, crawl_status(resume|dead))
    params = (update.name, update.health, update.total_posts, update.updated)
    cur.execute(cur.prep('execute procedure insert_blog_init_info(?, ?, ?, ?, ?)'), params)
    return

def fetch_blog_status(database):
    """retrieves info about blog status in Database"""
    # cur.prep("execute procedure check_blog_status(?)")
    # return db_blog_status 
    pass

def ping_blog_status(blog):
    """retrieves info from tumblr API concerning blog"""
    # fetch_blog_info(blog) returns:
    #     404 or any error -> dead or unknown (if only network error)
    #     lastupdated, total_posts, wiped if 0 post (or less than 10 posts?)
    # return status
    pass

def fetch_random_blog(database):
    """ Queries DB for a blog that is available """
    con = database.con
    cur = con.cursor()
    with fdb.TransactionContext(con):
        # sql = cur.prep("execute procedure fetch_one_blogname;")
        cur.execute("execute procedure fetch_one_blogname;")
        # cur.execute("select * from blogs;")

        return cur.fetchone()

        


def populate_db_with_archives(database, archivepath):
    """read archive list and populate the OLD_1280 table"""
    con = fdb.connect(database=database.db_filepath, user=database.username, password=database.password)
    cur = con.cursor()
    oldfiles = readlines(archivepath)
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


def populate_db_with_blogs(database, blogpath):
    """read csv list or blog, priority and insert them into BLOGS table """
    con = fdb.connect(database=database.db_filepath, user=database.username, password=database.password)
    cur = con.cursor()
    t0 = time.time()
    with fdb.TransactionContext(con):
        insert_statement = cur.prep("execute procedure insert_blogname(?)")

        for blog, priority in read_csv_bloglist(blogpath):
            params = (blog.rstrip() , priority)
            try:
                cur.execute(insert_statement, params)
            except fdb.fbcore.DatabaseError as e:
                if "violation of PRIMARY or UNIQUE KEY" in e.__str__():
                    print("Error when inserting blog: " + blog + " is already recorded.")
        con.commit()

    t1 = time.time()
    print('Inserting records into BLOGS Took %.2f ms' % (1000*(t1-t0)))


def read_csv_bloglist(blogpath):
    """yields a tuple of blog, prio
    prio is None if there is no comma"""

    with open(blogpath, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            priority = None
            blog = row[0]
            if len(row) > 1:
                priority = row[-1]
                
            yield (blog, priority)

def readlines(filepath):
    """read a newline separated file list"""

    with open(filepath, 'r') as f:
        data = f.read()
    return data


def create_blank_database(database):
    """Creates a new blank DB file and populates with tables"""
    create_blank_db_file(database)
    populate_db_with_tables(database)


def test_update_table(database):
    """feed testing data"""
    update = tumblr_client.parse_json_response(json.load(open\
    (SCRIPTDIR + "tools/test/vgf_july_reblogfalse_dupe.json", 'r')))
    # con = fdb.connect(database=database.db_filepath, user=database.username, password=database.password)
    con = database.con
    cur = con.cursor()
    insertstmt = cur.prep('insert into URLS (file_url,post_id,remote_id) values (?,?,?)')
    t0 = time.time()
    with fdb.TransactionContext(con):
        for post in update.trimmed_posts_list:
            try:
                # operation = cur.prep("execute procedure insert_post(?,?,?,?,?,?)")
                # params = (post['id'], post['blog_name'], post['post_url'], post['date'], \
                # post['remote_id'], post['reblogged_blog_name'])
                # cur.execute(operation, params)

                params = (post['id'], post['blog_name'], post['post_url'], \
                post['date'], post['timestamp'], post['remote_id'], post['reblogged_blog_name'], \
                post['remote_content'], post['photos'])
                print(post)

                cur.callproc('insert_post', itemgetter(0,1,2,3,5,6)(params))

                cur.callproc('insert_context', itemgetter(0,4,7,5)(params))
                for photo in post['photos']:
                    paramlist = (photo, post['id'], post['remote_id'])
                    try:
                        cur.execute(insertstmt, paramlist)
                    except Exception as e:
                        print(BColors.BLUEOK + BColors.FAIL + "ERROR inserting url:" + str(e) + BColors.ENDC)
                        continue
            except Exception as e:
                print(BColors.FAIL + "ERROR executing procedures for post and context:" + str(e) + BColors.ENDC)
                break
        else:
            print("NO BREAK OCCURED IN FOR LOOP, COMMITTING")
            con.commit()

    t1 = time.time()
    print('Procedures to insert took %.2f ms' % (1000*(t1-t0)))





if __name__ == "__main__":
    blogs_toscrape = SCRIPTDIR + "tools/blogs_toscrape_test.txt"
    archives_toload = SCRIPTDIR +  "tools/1280_files_list.txt"
    database = Database(db_filepath="/home/firebird/tumblrmapper_test.fdb", username="sysdba", password="masterkey")
    
    # create_blank_database(database)
    # populate_db_with_blogs(database, blogs_toscrape)
    # Optional archives too
    # populate_db_with_archives(database, archives_toload)
    database.connect()
    # test_update_table(database)
    fetch_random_blog(database)
