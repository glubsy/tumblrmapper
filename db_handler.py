#!/usr/bin/env python3.6
import csv
import html.parser
import json
import logging
import os
import sys
import time
import traceback
from collections import defaultdict
from operator import itemgetter
# import html.parser
from urllib import parse
import fdb
import archive_lists
import instances
# import cProfile
from constants import BColors

HAS_RE2 = False
try:
    import re2 as re #pip install git+https://github.com/andreasvc/pyre2.git to fix potential catastrophic backtracking failures
    HAS_RE2 = True
except ImportError:
    HAS_RE2 = False
    logging.debug(f'{BColors.RED}Could not load re2 module!{BColors.ENDC}')
    import re
# else:
#     re.set_fallback_notification(re.FALLBACK_WARNING)
http_url_simple_re = re.compile(r'"(https?(?::\/\/|%3A%2F%2F).*?)"', re.I)
# for single line with http
http_url_single_re = re.compile(r'(https?(?::\/\/|%3A%2F%2F).*?)(?:\s)*?$', re.I)
# matches quoted, between quotes, before html tags
http_url_super_re = re.compile(r'(?:\"(https?(?::\/\/|%3A%2F%2F).*?)(?:\")(?:<\/)*?)|(?:(https?:\/\/.*?)(?:(?:\s)|(?:<)))', re.I)
# matches all urls, even without http or www! https://gist.github.com/uogbuji/705383
if HAS_RE2:
    http_url_uber_re = re.compile(r'\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'\"\\\/.,<>?\xab\xbb]))', re.I)
else:
    http_url_uber_re = re.compile(r'\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'\"\\\/.,<>?\xab\xbb\u201c\u201d\u2018\u2019]))', re.I)
#repattern_tumblr_redirect = re.compile(r't\.umblr\.com\/redirect\?z=(.*)(?:&|&amp;)t=.*', re.I) # obsolete
repattern_tumblr_redirect = re.compile(r'.*t\.umblr\.com(?:\/|%2F)redirect(?:\?|%3F)z(?:=|%3D)(.*)(?:&|&amp;)t=.*', re.I)

# Deprecated:
repattern_tumblr = re.compile(r'(tumblr_.*)_.*\..*', re.I) #eliminate '_resol.ext'

repattern_revisions = re.compile(r'(tumblr_.*?)(?:_r\d)?\s*$', re.I) #elimitane '_r1'

urls_blacklist_filter = ['://tmblr.co/', 'strawpoll.me', 'youtube.com', 'wikipedia.org', 'wikia.com']

htmlparser = html.parser.HTMLParser()

#DEBUG:
# FILTERED_URL_GLOBAL_COUNT = set()

class Database():
    """handle the db file itself, creating everything
    Args are: filepath, user, password, bloglist, archives, host=None"""


    def __init__(self, *args, **kwargs):
        """initialize with environment"""
        self.host = kwargs.get('db_host', 'localhost') #not implemented
        self.db_filepath = kwargs.get('db_filepath', None)
        self.username = kwargs.get('username', "sysdba")
        self.password = kwargs.get('password', "masterkey")
        self.con = []
        if not self.db_filepath:
            raise "Need a filepath for database"

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ close all remaining connections"""
        logging.info(BColors.BLUE + "Closing connections to DB" + BColors.ENDC)
        for con in self.con:
            con.close()


    def connect(self):
        """initialize connection to remote DB"""
        if self.host == 'localhost':
            con = fdb.connect(database=self.db_filepath,
                              user=self.username, password=self.password)
        else:
            con = fdb.connect(database=str(self.host + ":" + self.db_filepath),
                              user=self.username, password=self.password)
        self.con.append(con)
        return con


    def close_connection(self, con=None):
        if not con:
            for connection in self.con:
                connection.close()
                self.con.remove(connection)
        else:
            self.con.remove(con)
            return con.close()



def create_blank_database(database):
    """Creates a new blank DB file and populates with tables"""

    # ("create database 'host:/temp/db.db' user 'sysdba' password 'pass'")
    c = r"create database " + r"'" + database.db_filepath + \
    r"' user '" + database.username + r"' password '" + database.password + r"'"

    try:
        fdb.create_database(c)
    except:
        raise

    populate_db_with_tables(database)

    logging.warning(BColors.BLUEOK + BColors.GREEN
    + "Done creating blank DB in: {0}"
    .format(database.db_filepath) + BColors.ENDC)


def populate_db_with_tables(database):
    """Create our tables and procedures here in the DB"""
    con = fdb.connect(database=database.db_filepath, \
                      user=database.username, password=database.password)
    with fdb.TransactionContext(con):
        #auto rollback if exception is raised, and no need to con.close() because automatic
        # cur = con.cursor()
        # Create domains
        con.execute_immediate("CREATE DOMAIN D_LONG_TEXT AS VARCHAR(500);")
        con.execute_immediate("CREATE DOMAIN D_URL AS VARCHAR(1000);")
        con.execute_immediate("CREATE DOMAIN D_POSTURL AS VARCHAR(300);")
        con.execute_immediate("CREATE DOMAIN D_AUTO_ID AS BIGINT;")
        con.execute_immediate("CREATE DOMAIN D_BLOG_NAME AS VARCHAR(60);")
        con.execute_immediate("CREATE DOMAIN D_EPOCH AS BIGINT;")
        con.execute_immediate("CREATE DOMAIN D_POST_NO AS BIGINT;")
        con.execute_immediate("CREATE DOMAIN D_SUPER_LONG_TEXT AS VARCHAR(32765)")
        con.execute_immediate("CREATE DOMAIN D_HASH AS VARCHAR(25)")
        con.execute_immediate("CREATE DOMAIN D_INLINE_HASH AS VARCHAR(45)")
        con.execute_immediate(
"""CREATE DOMAIN D_BOOLEAN AS smallint default 0
CHECK (VALUE IS NULL OR VALUE IN (0, 1, 2));""")

        # Create tables with columns
        # CRAWLING = 1 currently crawling, 2 temporarily stopped due to server-side error
        con.execute_immediate(
"""
CREATE TABLE BLOGS (
AUTO_ID         D_AUTO_ID PRIMARY KEY,
BLOG_NAME       D_BLOG_NAME,
HEALTH          varchar(5),
CRAWL_STATUS    varchar(10) DEFAULT NULL,
CRAWLING        D_BOOLEAN default 0,
TOTAL_POSTS     INTEGER,
POST_OFFSET     INTEGER,
POSTS_SCRAPED   INTEGER,
LAST_CHECKED    D_EPOCH,
LAST_UPDATE     D_EPOCH,
PRIORITY        smallint,
HASH            D_HASH,
INLINE_HASH     D_INLINE_HASH,
CONSTRAINT blognames_unique UNIQUE (BLOG_NAME) using index ix_blognames
);""")

        con.execute_immediate(
"""
CREATE TABLE CRAWLING(
BLOG_NAME D_BLOG_NAME PRIMARY KEY,
CRAWL_STATUS varchar(20)
);
""")


        con.execute_immediate(\
"""
CREATE TABLE POSTS (
POST_ID             D_POST_NO PRIMARY KEY,
REMOTE_ID           D_POST_NO,
ORIGIN_BLOGNAME     D_AUTO_ID NOT NULL,
REBLOGGED_BLOGNAME  D_AUTO_ID,
POST_URL            D_POSTURL NOT NULL,
POST_DATE           D_EPOCH,
NOTES               integer,
FOREIGN KEY(ORIGIN_BLOGNAME) REFERENCES BLOGS(AUTO_ID),
FOREIGN KEY(REBLOGGED_BLOGNAME) REFERENCES BLOGS(AUTO_ID)
CONSTRAINT remote_ids_ix
);""")

        # Create index for faster bulk updating and reading
        con.execute_immediate(
"""create index ix_remote_ids on posts (remote_id);""")

        con.execute_immediate(\
"""
CREATE TABLE CONTEXTS (
POST_ID         D_POST_NO,
REMOTE_ID       D_POST_NO UNIQUE,
TTIMESTAMP      D_EPOCH,
LATEST_REBLOG   D_POST_NO,
CONTEXT         D_SUPER_LONG_TEXT,
PRIMARY KEY(POST_ID),
FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID)
);""")
# to update
# if remote_id is null, then it's an original post, not reblogged, we store everything no problem
# if we already have that remote_id, we don't want to store context again -> we skip
# otherwise only IF the timestamp we hold is newer, then we UPDATE the context
# LATEST_REBLOG is the latest reblog that we used to update the timestamp and context fields of an original post
# that we had recorded.
# REMOTE_ID can be NULL! (allowed with unique)
# if we input an existing REMOTE_ID (with a new POST_ID, because reblogged by many POST_ID), then EXCEPTION!

        con.execute_immediate(
"""CREATE TABLE URLS (
FILE_URL             D_URL PRIMARY KEY,
POST_ID              D_POST_NO NOT NULL,
REMOTE_ID            D_POST_NO,
FOREIGN KEY(POST_ID) REFERENCES POSTS(POST_ID)
);""")
# if remote_id is null, it means it was not a reblog
# if it's not null, it's from a reblog,

        con.execute_immediate(
"""
CREATE TABLE OLD_1280 (
FILENAME varchar(60) PRIMARY KEY,
FILEBASENAME varchar(60),
PATH varchar(10000)
);""")

        # CREATE generators and triggers
        con.execute_immediate("CREATE SEQUENCE tBLOGS_autoid_sequence;")
        con.execute_immediate("CREATE SEQUENCE tBLOGS_autoid_sequence2;")
        con.execute_immediate("ALTER SEQUENCE tBLOGS_autoid_sequence2 RESTART WITH 9999;")
        # con.execute_immediate("CREATE SEQUENCE tGATHERED_BLOGS_autoid_sequence;")
        # con.execute_immediate(\
        #     """CREATE TRIGGER tGATHERED_BLOGS_AUTOINC FOR BLOGS
        #     ACTIVE BEFORE INSERT POSITION 0
        #     AS BEGIN NEW.AUTO_ID = next value for tBLOGS_autoid_sequence; END""")

        # CREATE procedures
        # Records given blogname into BLOG table, increments auto_id,
        # decrements auto_id in case an exception occured (on non unique inputs)
        con.execute_immediate(
"""
CREATE OR ALTER PROCEDURE INSERT_BLOGNAME (
    I_BLOGNAME D_BLOG_NAME,
    I_CRAWL_STATUS VARCHAR(10) DEFAULT 'new',
    I_PRIO SMALLINT DEFAULT null )
AS
declare variable v_generated_auto_id d_auto_id;
BEGIN
if (exists (select BLOG_NAME from BLOGS where (BLOG_NAME = :i_blogname))) THEN
BEGIN
    if ((select CRAWL_STATUS from BLOGS where (BLOG_NAME = :i_blogname)) is NULL) then
    begin
        update BLOGS set CRAWL_STATUS = :i_crawl_status, PRIORITY = :i_prio
        where BLOG_NAME = :i_blogname;
        exit;
    END
    ELSE begin
        exit; /* just in case, I don't know...*/
    end
end
else
begin
    v_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence, 1);
    INSERT into BLOGS (AUTO_ID, BLOG_NAME, CRAWL_STATUS, PRIORITY)
    values (:v_generated_auto_id, :i_blogname, :i_crawl_status, :i_prio);
    exit;
end
WHEN GDSCODE unique_key_violation
DO begin
    v_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence, -1);
    exception;
    end
END
""")
        # insert gathered blog names
        con.execute_immediate(
"""
CREATE OR ALTER PROCEDURE INSERT_BLOGNAME_GATHERED (
    i_blogname D_BLOG_NAME,
    i_new varchar(10) default NULL
    )
returns (o_generated_auto_id d_auto_id,
o_crawl_status varchar(10))
AS
BEGIN
if (not exists (select AUTO_ID from BLOGS where (BLOGS.BLOG_NAME = :i_blogname)))
THEN begin
    o_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence2, 1);
    INSERT into BLOGS (AUTO_ID, BLOG_NAME, CRAWL_STATUS) values (:o_generated_auto_id, :i_blogname, :i_new);
    suspend;
    exit;
    end
ELSE
BEGIN
    select AUTO_ID, CRAWL_STATUS from BLOGS where BLOGS.BLOG_NAME = :i_blogname into :o_generated_auto_id, o_crawl_status;
    if ((:i_new is not NULL ) and (:o_crawl_status is NULL)) then
        begin
            update BLOGS set CRAWL_STATUS = :i_new where BLOG_NAME = :i_blogname;
            suspend;
            exit;
        end
    else
        suspend;
        exit;
END
WHEN GDSCODE unique_key_violation
DO
o_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence2, -1);
END
""")

        con.execute_immediate(
"""
CREATE OR ALTER PROCEDURE reset_crawl_status
 ( i_blog_name d_blog_name, i_reset_type varchar(10) )
AS declare variable v_crawl varchar(10) default null;
BEGIN
select (CRAWL_STATUS) from BLOGS where (BLOG_NAME = :i_blog_name) into :v_crawl;
if (v_crawl is not null) THEN
update BLOGS set CRAWL_STATUS = :i_reset_type where (BLOG_NAME = :i_blog_name);
END
""")

        # Inserts a post and all its metadata
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE INSERT_POST (
    I_POSTID D_POST_NO,
    I_BLOG_ORIGIN D_BLOG_NAME,
    I_POST_URL D_POSTURL,
    I_POST_DATE D_EPOCH,
    I_REMOTEID D_POST_NO DEFAULT null,
    I_REBLOGGED_BLOG_NAME D_BLOG_NAME DEFAULT null,
    I_NOTES integer default null)
AS
declare variable v_blog_origin_id d_auto_id;
declare variable v_fetched_reblogged_blog_id d_auto_id default null;
declare variable v_b_update_gathered d_boolean default 0;
BEGIN

select AUTO_ID from BLOGS where BLOG_NAME = :i_blog_origin into :v_blog_origin_id;
if (v_blog_origin_id is null) THEN
begin
    select O_GENERATED_AUTO_ID from INSERT_BLOGNAME_GATHERED(:i_blog_origin)
    into :v_blog_origin_id;
END

if (:i_reblogged_blog_name is not null)
THEN
select O_GENERATED_AUTO_ID from INSERT_BLOGNAME_GATHERED(:i_reblogged_blog_name)
into :v_fetched_reblogged_blog_id;

update or insert into posts (POST_ID, POST_URL, POST_DATE, REMOTE_ID,
ORIGIN_BLOGNAME, REBLOGGED_BLOGNAME, NOTES)
values (:i_postid, :i_post_url, :i_post_date, :i_remoteid,
:v_blog_origin_id, :v_fetched_reblogged_blog_id, :i_notes)
matching (post_id, ORIGIN_BLOGNAME);
END
""")

        # Inserts context, update if already present with latest reblog's values
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE INSERT_CONTEXT (
    I_POST_ID D_POST_NO NOT NULL,
    I_TIMESTAMP D_EPOCH,
    I_REMOTE_ID D_POST_NO DEFAULT null,
    I_CONTEXT D_SUPER_LONG_TEXT DEFAULT null )
AS
BEGIN
if (:i_remote_id is null) then
    begin /* we store everything, it's an original post*/
        insert into CONTEXTS (POST_ID, TTIMESTAMP, REMOTE_ID, CONTEXT)
        values (:i_post_id, :i_timestamp, :i_remote_id, :i_context);
    end
else
    begin /* we might not want to keep it*/
        if (exists (select (REMOTE_ID) from CONTEXTS where (REMOTE_ID = :i_remote_id))) then /*if remote_id already in remote_id col*/
            begin                                                                           /*keep the latest one, update if newer*/
                if (:i_timestamp > (select (TTIMESTAMP) from CONTEXTS where (REMOTE_ID = :i_remote_id))) then
                begin
                    update CONTEXTS
                    set CONTEXT = :i_context,
                        TTIMESTAMP = :i_timestamp,
                        LATEST_REBLOG = :i_post_id
                        where (REMOTE_ID = :i_remote_id);
                    exit;
                end
            end
        else
            begin
                insert into CONTEXTS (POST_ID, TTIMESTAMP, REMOTE_ID, CONTEXT)
                values (:i_post_id, :i_timestamp, :i_remote_id, :i_context);
                exit;
            end
        end
--when any do
--exit;
END
""")

        # replaces an entry if i_post_id and i_remote_id are equal
        # we consider it to be a self reblog, which is the most up to date
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE insert_url
 ( i_url D_URL,
 i_post_id d_post_no,
 i_remote_id d_post_no default null)
AS
declare variable v_rid d_post_no;
BEGIN
if ((i_remote_id is null)) THEN
begin
    update or insert into URLS (FILE_URL, POST_ID, REMOTE_ID)
    values (:i_url, :i_post_id, :i_remote_id) MATCHING (FILE_URL, POST_ID);
    exit;
end
if ((i_remote_id = i_post_id) and (i_remote_id is not null)) then
begin /*self reblog, priority*/

    select REMOTE_ID from URLS
    where URLS.FILE_URL = :i_url into v_rid;

    if (v_rid = :i_post_id) THEN
    begin
    update URLS set POST_ID = :i_post_id, REMOTE_ID = :i_remote_id
    where file_url = :i_url;
    exit;
    end
end
ELSE begin
insert into URLS (FILE_URL, POST_ID, REMOTE_ID)
values (:i_url, :i_post_id, :i_remote_id);
--when ANY do /*supress any exception, TODO: capture duplicate to avoid overhead*/
--exit;
end
END
""")

        # Fetch new blogs, priority first
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE FETCH_ONE_BLOGNAME
( i_status_type varchar(10) default 'resume')
RETURNS (
    O_NAME D_BLOG_NAME,
    O_OFFSET INTEGER,
    O_HEALTH VARCHAR(5),
    O_STATUS VARCHAR(10),
    O_TOTAL INTEGER,
    O_SCRAPED INTEGER,
    O_CHECKED D_EPOCH,
    O_UPDATED D_EPOCH )
AS
BEGIN
if (exists (select BLOG_NAME from BLOGS where ((CRAWL_STATUS = :i_status_type) and (CRAWLING = 0)))) then
    begin
    for select BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED, LAST_UPDATE
    from BLOGS where ((CRAWL_STATUS = :i_status_type) and (CRAWLING = 0)) order by PRIORITY desc nulls last ROWS 1 with lock
    into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked, :o_updated
    as cursor cur do
        if (:o_status = 'resume') THEN
        begin
            update BLOGS set CRAWLING = 1 where current of cur;
            insert into CRAWLING (BLOG_NAME, CRAWL_STATUS) values (:o_name, 'progress');
        end
        else if (:o_status = 'new') THEN
        begin
            update BLOGS set CRAWL_STATUS = 'init', CRAWLING = 1 where current of cur;
            insert into CRAWLING (BLOG_NAME, CRAWL_STATUS) values (:o_name, 'init');
        end
    exit;
    end
END
""")

        # Fetch DONE blogs ordered by last scraped
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE FETCH_ONE_DONE_BLOGNAME
RETURNS (
    O_NAME D_BLOG_NAME,
    O_OFFSET INTEGER,
    O_HEALTH VARCHAR(5),
    O_STATUS VARCHAR(10),
    O_TOTAL INTEGER,
    O_SCRAPED INTEGER,
    O_CHECKED D_EPOCH,
    O_UPDATED D_EPOCH )
AS
BEGIN
if (exists (select BLOG_NAME from BLOGS where (CRAWL_STATUS = 'DONE'))) then
    BEGIN
    for select BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED, LAST_UPDATE
        from BLOGS where (CRAWL_STATUS = 'DONE')
        order by (LAST_CHECKED) asc nulls last ROWS 1 with lock
        into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked, :o_updated
        as cursor tcur do
        begin
            update BLOGS set CRAWL_STATUS = 'resume', CRAWLING = 1 where current of tcur;
            insert into CRAWLING (BLOG_NAME, CRAWL_STATUS) values (:o_name, 'progress');
        end
        exit;
    END
END
""")

        # Reset DONE blogs to "resume" for which number of posts scraped is more than 100
        # less then the total
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE RESET_INCOMPLETE_BLOGNAMES
RETURNS (
    O_COUNT integer)
AS
declare variable O_NAME D_BLOG_NAME;
declare variable O_OFFSET INTEGER;
declare variable O_HEALTH VARCHAR(5);
declare variable O_STATUS VARCHAR(10);
declare variable O_TOTAL INTEGER;
declare variable O_SCRAPED INTEGER;
declare variable O_CHECKED D_EPOCH;
declare variable O_UPDATED D_EPOCH;
declare variable O_actual integer;
BEGIN
o_count = 0;
    for select BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED, LAST_UPDATE
        from BLOGS where ((CRAWL_STATUS = 'DONE') and (POSTS_SCRAPED < TOTAL_POSTS and TOTAL_POSTS - POSTS_SCRAPED > 100))
        into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked, :o_updated
        as cursor tcur do
        begin
            select count(*) from (select POST_ID from POSTS where ORIGIN_BLOGNAME =
 (select auto_id from BLOGS where BLOG_NAME = :o_name)) into :o_actual;
            if (:o_total - :o_actual > 50) then
            begin
                o_count = :o_count + 1;
                update BLOGS set CRAWL_STATUS = 'resume', POST_OFFSET = :o_scraped, posts_scraped = :o_actual where current of tcur;
            end
        end
suspend;
exit;
END
""")


#         # Testing method with CRAWLING table (not used, ugly code)
#         con.execute_immediate(
# """
# CREATE OR ALTER PROCEDURE FETCH_ONE_BLOGNAME_CRAWLING_TABLE
# (   i_status_type varchar(10) default 'resume'
# )
# RETURNS (
#     O_NAME D_BLOG_NAME,
#     O_OFFSET INTEGER,
#     O_HEALTH VARCHAR(5),
#     O_STATUS VARCHAR(10),
#     O_TOTAL INTEGER,
#     O_SCRAPED INTEGER,
#     O_CHECKED D_EPOCH,
#     O_UPDATED D_EPOCH )
# AS
# declare variable v_counter bigint default -1;
# BEGIN
# while (v_counter >= -2) do
# begin
#     o_name = null;
#     if (v_counter = -2) THEN
#     begin
#         i_status_type = 'new';
#         v_counter = -1;
#     end
#     v_counter = :v_counter + 1;
#     select  first 1 skip (:v_counter) BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED, LAST_UPDATE
#     from BLOGS where ((CRAWL_STATUS = :i_status_type )) order by PRIORITY desc nulls last
#     --with lock
#     into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked, :o_updated;
#     if (:o_name is null) THEN
#     begin
#         v_counter = -2;
#     end
#     if (:v_counter != -2) then
#     begin
#         if (not exists (select blog_name from CRAWLING where blog_name = :o_name)) then
#         begin
#             insert into CRAWLING (BLOG_NAME, CRAWL_STATUS) values (:o_name, 'progress');
#             if (:o_status = 'new') then
#             begin
#                 update CRAWLING set CRAWL_STATUS = 'init' where BLOG_NAME = :o_name;
#             end
#             else
#             if (:o_status = 'DONE') then
#             begin
#                 update BLOGS set CRAWL_STATUS = 'resume' where BLOG_NAME = :o_name;
#             end
#             exit;
#         end
#     end
# end
# END
# """)



        # for dead blog accounts or account sorted by priority
        # fetch reblogged posts, only if they have no notes recorded
        # returns nothing when all have been processed
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE FETCH_ALL_BLOG_S_POSTS
(i_param varchar(10))
RETURNS (
    O_POST_ID D_POST_NO,
    O_REMOTE_ID D_POST_NO,
    O_ORIGIN_ID D_AUTO_ID,
    O_REBLOGGED_ID D_AUTO_ID,
    O_NOTES INTEGER,
    O_ORIGIN_ID2 D_AUTO_ID,
    O_ORIGIN_NAME D_BLOG_NAME,
    O_REBLOGGED_ID2 D_AUTO_ID,
    O_REBLOGGED_NAME2 D_BLOG_NAME )
AS
declare variable v_blog D_BLOG_NAME;
BEGIN
    if (:i_param = 'dead') then
    begin
        for select first 1 (BLOG_NAME) from BLOGS where (((HEALTH = 'DEAD') and (CRAWLING != 1))
        or ((HEALTH = 'WIPED' and TOTAL_POSTS <= 2) and (CRAWLING != 1))) into :v_blog do
        begin
            update blogs set CRAWLING = 1 where blogs.blog_name = :v_blog; --avoid rerolling it
            insert into CRAWLING (BLOG_NAME, CRAWL_STATUS) values (:v_blog, 'progress');
            if (exists (select first 1 * from FETCH_DEAD_POSTS(:v_blog) where o_notes is null)) THEN
            begin
                for select * from FETCH_DEAD_POSTS(:v_blog) where o_notes is null
                into :o_post_id, :o_remote_id, :o_origin_id, :o_reblogged_id, :o_notes,
                :o_origin_id2, :o_origin_name, :o_reblogged_id2, :o_reblogged_name2
                do
                suspend;
                exit;
            end
            else
            O_REBLOGGED_NAME2 = :v_blog;
            O_post_id = 0; -- no reblogs found found
            suspend;
        end
    end
    else if (:i_param = 'priority') then
    begin
        for select first 1 (BLOG_NAME) from BLOGS where ((PRIORITY is not null) and (CRAWLING != 1))
        order by PRIORITY desc nulls last into :v_blog do
        begin
            update blogs set CRAWLING = 1 where blogs.blog_name = :v_blog; --avoid rerolling it
            insert into CRAWLING (BLOG_NAME, CRAWL_STATUS) values (:v_blog, 'progress');
            if (exists (select first 1 * from FETCH_DEAD_POSTS(:v_blog) where o_notes is null)) THEN
            begin
                for select * from FETCH_DEAD_POSTS(:v_blog) where o_notes is null
                into :o_post_id, :o_remote_id, :o_origin_id, :o_reblogged_id, :o_notes,
                :o_origin_id2, :o_origin_name, :o_reblogged_id2, :o_reblogged_name2
                do
                suspend;
                exit;
            end
            else
            O_REBLOGGED_NAME2 = :v_blog;
            O_post_id = 0; -- no reblogs found found
            suspend;
        end
    end
end
""")

        # selectable procedure!
        # O_ORIGIN_ID2 / O_ORIGIN_NAME = the reblogged blog (if there's one, otherwise null)
        # O_REBLOGGED_ID2 / O_REBLOGGED_NAME2 = the actual poster
        # WARNING the names are mixed up, reblogged_name2 is actually misleading
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE FETCH_DEAD_POSTS (
    I_NAME D_BLOG_NAME )
RETURNS (
    O_POST_ID D_POST_NO,
    O_REMOTE_ID D_POST_NO,
    O_ORIGIN_ID D_AUTO_ID,
    O_REBLOGGED_ID D_AUTO_ID,
    O_NOTES INTEGER,
    O_ORIGIN_ID2 D_AUTO_ID,
    O_ORIGIN_NAME D_BLOG_NAME,
    O_REBLOGGED_ID2 D_AUTO_ID,
    O_REBLOGGED_NAME2 D_BLOG_NAME )
AS
begin
    for select p.post_id, p.remote_id, p.ORIGIN_BLOGNAME, p.REBLOGGED_BLOGNAME, p.notes, c1.auto_id, c1.BLOG_NAME, c2.auto_id, c2.BLOG_NAME
    from posts as p
    left join blogs as c1 on c1.auto_id = p.reblogged_blogname or (c1.auto_id = p.ORIGIN_BLOGNAME and c1.auto_id = p.REBLOGGED_BLOGNAME)
    left join blogs as c2 on c2.auto_id = p.origin_blogname or (c2.auto_id = p.REBLOGGED_BLOGNAME and c2.auto_id = p.ORIGIN_BLOGNAME)
    where (p.reblogged_blogname = (select auto_id from blogs where blogs.blog_name = :i_name))
    or (p.ORIGIN_BLOGNAME = (select auto_id from blogs where blogs.blog_name = :i_name))
    into :o_post_id, :o_remote_id, :o_origin_id, :o_reblogged_id, :o_notes,
    :o_origin_id2, :o_origin_name, :o_reblogged_id2, :o_reblogged_name2
    do
    suspend;
end""")

        # Update info fetched from API
        # args:  (blogname, health(UP,DEAD,WIPED), totalposts, updated_timestamp, status(resume, dead) )
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE update_blog_info_init (
    i_name D_BLOG_NAME,
    i_health varchar(5),
    i_total integer,
    i_updated d_epoch,
    i_status varchar(10) default 'resume',
    i_crawling d_boolean default 0)
RETURNS(
    O_health varchar(5),
    O_total_posts INTEGER,
    O_updated D_EPOCH,
    O_last_checked D_EPOCH,
    O_offset INTEGER,
    O_scraped INTEGER)
AS declare variable v_checked d_epoch;
BEGIN
select DATEDIFF(second FROM timestamp '1/1/1970 00:00:00' TO current_timestamp)
from rdb$database into :v_checked;
update BLOGS set
HEALTH = :i_health,
TOTAL_POSTS = :i_total,
CRAWL_STATUS = :i_status,
CRAWLING = :i_crawling,
LAST_UPDATE = :i_updated,
LAST_CHECKED = :v_checked
where BLOG_NAME = :i_name
returning old.HEALTH, old.total_posts, old.LAST_UPDATE, old.LAST_CHECKED, POST_OFFSET, POSTS_SCRAPED
into O_health, O_total_posts, O_updated, O_last_checked, O_offset, O_scraped;
if (:i_crawling != 0) then
    insert into CRAWLING (BLOG_NAME, CRAWL_STATUS) values (:i_name, 'progress');
END""")

        # Update whenever API gives us new different values than what we already had
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE update_blog_info (
    i_name D_BLOG_NAME,
    i_health varchar(5),
    i_total integer,
    i_updated d_epoch,
    i_offset integer,
    i_scraped integer,
    i_status varchar(10) default 'resume',
    i_crawling d_boolean default 0)
RETURNS(
    O_health varchar(5),
    O_total_posts INTEGER,
    O_updated D_EPOCH,
    O_last_checked D_EPOCH,
    O_offset INTEGER,
    O_scraped INTEGER)
AS declare variable v_checked d_epoch;
BEGIN
    select DATEDIFF(second FROM timestamp '1/1/1970 00:00:00' TO current_timestamp)
    from rdb$database into :v_checked;
    update BLOGS set
    HEALTH = :i_health,
    TOTAL_POSTS = :i_total,
    CRAWL_STATUS = :i_status,
    CRAWLING = :i_crawling,
    LAST_UPDATE = :i_updated,
    LAST_CHECKED = :v_checked,
    POST_OFFSET = :i_offset,
    POSTS_SCRAPED = :i_scraped
    where BLOG_NAME = :i_name
    returning old.HEALTH, old.TOTAL_POSTS, old.LAST_UPDATE, old.LAST_CHECKED, old.POST_OFFSET, old.POSTS_SCRAPED
    into O_health, O_total_posts, O_updated, O_last_checked, O_offset, O_scraped;
    if (:i_crawling != 0) then
        update or insert into CRAWLING (BLOG_NAME, CRAWL_STATUS)
        values (:i_name, 'progress') MATCHING (BLOG_NAME);
    else if (:i_crawling = 0) then
        delete from CRAWLING where BLOG_NAME = :i_name;
END""")


        # called when quitting script, or done scraping total_posts
        con.execute_immediate(\
"""CREATE OR ALTER PROCEDURE reset_blog_crawling_status (i_name d_blog_name)
AS BEGIN
delete from CRAWLING where BLOG_NAME = :i_name;
update BLOGS set CRAWLING = 0 where BLOG_NAME = :i_name;
END""")



        # Uses the CRAWLING table to reset BLOGS rows on script startup
        con.execute_immediate(
"""
CREATE OR ALTER PROCEDURE reset_crawl_status_all
AS
declare variable v_status varchar(10);
declare variable v_blog d_blog_name;
BEGIN
for select BLOG_NAME, CRAWL_STATUS from CRAWLING into :v_blog, :v_status
do
BEGIN
    if (:v_status = 'init') THEN
        update BLOGS set CRAWL_STATUS = 'new', CRAWLING = NULL where blog_name = :v_blog;
    else if (:v_status = 'progress') THEN
        update BLOGS set CRAWLING = 0 where blog_name = :v_blog;
END
delete from CRAWLING;
END
""")

        # insert_archive
        # 'inputs are (filename without revision, filename, path origin) if path is already
        # in the path col, append it with ## before it. Returns 1 if filename was found.
        con.execute_immediate(\
"""
create or alter procedure insert_archive (
i_f varchar(60),
i_fb varchar(60),
i_p varchar(100))
returns (f D_BOOLEAN)
as
declare variable v_p varchar(10000);
begin
if (exists (select * from OLD_1280 where filename = :i_f )) THEN
begin
    f = 1;
    select path from OLD_1280 where filename = :i_f into v_p;
    if (:v_p not similar to :i_p ) then
        if (:v_p not like '%'||:i_p||'%') then
            update OLD_1280 set path = TRIM(:v_p) ||'##'|| :i_p where filename = :i_f;
        else
            exit;
    else
        exit;
end
ELSE
    begin
        INSERT INTO OLD_1280 (FILENAME, FILEBASENAME, PATH) VALUES (:i_f,:i_fb,:i_p);
    end
end""")

        # Create views
        # con.execute_immediate(
        # "CREATE VIEW v_posts ( \
        # POST_ID, REMOTE_ID, BLOG_ORIGIN, BLOG_REBLOGGED, POST_URL, POST_DATE) \
        # AS SELECT \
        # POST_ID, REMOTE_ID, BLOG_ORIGIN, BLOG_REBLOGGED, POST_URL, POST_DATE, AUTO_ID, BLOG_NAME \
        # FROM POSTS, GATHERED_BLOGS, BLOGS \
        # WHERE POSTS.BLOG_ORIGIN = BLOGS.AUTO_ID, POSTS.BLOG_REBLOGGED = GATHERED_BLOGS.BLOG_NAME \
        # );")

def update_db_with_archives(database, archivepath, use_pickle=True):
    """Reads the trimmed archive list and updates DB table OLD_1280"""
    con = database.connect()
    delete_all_1280_archives(con)
    cur = con.cursor()
    t0 = time.time()
    dupecount = 0
    # tuple of lists:
    if use_pickle:
        oldfiles = archive_lists.readfile_pickle(archivepath)
    else:
        oldfiles = archive_lists.readfile(archivepath, evaluate=True)

    with fdb.TransactionContext(con):
        for item in oldfiles:
            match = repattern_revisions.search(item[0])
            if match:
                trimmeditem = match.group(1)
                # if trimmeditem != item[0]:
                #     print("{0} -> {1}".format(item[0], trimmeditem))
            else:
                trimmeditem = item[0]

            params = (item[0], trimmeditem, item[1])
            try:
                # cur.execute("INSERT INTO OLD_1280 (FILENAME, FILEBASENAME, PATH) VALUES (?,?,?)",
                # params)
                cur.callproc('insert_archive', params)
                dupecount += cur.fetchone()[0]
            except fdb.DatabaseError as e:
                logging.error(BColors.FAIL + "Error while inserting archive: {}"
                .format(repr(e)))
        con.commit()
        logging.warning(BColors.BLUE + "Total archives to insert: {0}. Found {1} items already present \
while inserting archives. Inserted {2} new items."
        .format(len(oldfiles), dupecount, len(oldfiles) - dupecount) + BColors.ENDC)
    t1 = time.time()
    logging.warning(BColors.BLUE + "Inserting records into OLD_1280 Took %.2f ms"
                  % (1000*(t1-t0)) + BColors.ENDC)


def delete_all_1280_archives(con):
    """Remove all records in 1280_archives table"""
    cur = con.cursor()
    print("Removing all 1280 archives")
    try:
        cur.execute(r'delete from OLD_1280;')
    except BaseException as e:
        logging.debug(f'Error while deleting 1280 archives: {e}')
    finally:
        con.commit()
    print("Removed all from 1280 archives")


# def populate_db_with_archives(database, archivepath):
#     """Reads archive list with full _1280.jpg and populate the OLD_1280 table
#     with the base filename with and without revision (_r1, _r2) nor extension
#     : DEPRECATED"""
#     con = database.connect()
#     cur = con.cursor()
#     oldfiles = readlines(archivepath)

#     t0 = time.time()

#     with fdb.TransactionContext(con):
#         argsseq = list()
#         for line in oldfiles.splitlines():
#             reresult = repattern_tumblr.search(line)
#             basefilename = reresult.group(1) #tumblr_azec_azceniaoiz1_r1
#             reresult2 = repattern_revisions.search(basefilename)
#             if reresult2:
#                 basefilename = reresult2.group(1) #tumblr_azec_azceniaoiz1
#             argsseq.append((line, basefilename))

#         sql = cur.prep("INSERT INTO OLD_1280 (FILENAME, FILEBASENAME) VALUES (?,?)")
#         cur.executemany(sql, argsseq)
#         con.commit()

#     t1 = time.time()
#     logging.debug(BColors.BLUE + "Inserting records into OLD_1280 Took %.2f ms"
#                   % (1000*(t1-t0)) + BColors.ENDC)


def populate_db_with_blogs(database, blogpath):
    """read csv list or blog, priority and insert them into BLOGS table """
    con = fdb.connect(database=database.db_filepath,
                      user=database.username, password=database.password)
    cur = con.cursor()
    t0 = time.time()
    dupecount = 0
    itemcount = 0
    with fdb.TransactionContext(con):
        insert_statement = cur.prep("""execute procedure insert_blogname(?,?,?)""")

        for blog, priority in read_csv_bloglist(blogpath):
            itemcount += 1
            params = (blog.rstrip() , 'new', priority)
            try:
                cur.execute(insert_statement, params)
            except fdb.fbcore.DatabaseError as e:
                if "violation of PRIMARY or UNIQUE KEY" in e.__str__():
                    dupecount += 1
                    logging.debug(BColors.FAIL + "Error" + BColors.BLUE
                    + " inserting {0}: duplicate.".format(blog) + BColors.ENDC)
        if dupecount > 0:
            logging.warning(BColors.BLUE + "Found {0} blogs already recorded."
            .format(dupecount) + BColors.ENDC)
        con.commit()

    t1 = time.time()
    logging.debug('{0} Inserting {1} records into BLOGS Took {2:.2f} ms{3}'.format(
        BColors.BLUE, itemcount, 1000*(t1-t0), BColors.ENDC))


def read_csv_bloglist(blogpath):
    """yields a tuple of blog, prio
    prio is None if there is no comma"""

    with open(blogpath, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if row[0].startswith('#'):
                continue
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


def fetch_random_blog(database, con, status_req=None):
    """ Queries DB for a blog that is available, with its crawl_status = type
    [new|resume|DONE]
    By default, looks for any "resume", then "new", then "DONE".
    If status_req is new or resume, DONE blogs will be skipped.
    If status_req is "DONE", will look for DONE first, then "resume" then "new".
    returns: name, offset, health, status, total posts,
    scraped posts, last checked, last updated
    """
    cur = con.cursor()
    if status_req is None:
        status_try_order = ["resume", "new", "DONE"]
    elif status_req == "DONE":
        status_try_order = ["DONE", "resume", "new"]
    else: # only asked for new/resume, not DONE
        status_try_order = ["resume", "new"]
    # with fdb.TransactionContext(con):

    for status_type in status_try_order:
        row = (None,) * 8
        try:
            if status_type != "DONE":
                cur.callproc("fetch_one_blogname", (status_type,))
            else:
                cur.callproc("fetch_one_done_blogname")
            row = cur.fetchone()
        except BaseException as e:
            logging.debug(f"{BColors.FAIL}Exception when fetching one blog: {e}{BColors.ENDC}")
            return (None,) * 8
        finally:
            con.commit()

        if row[0] is not None:
            return row


def fetch_all_blog_s_posts(database, con, priority='dead'):
    """ Queries DB for either a dead blog or blog sorted by priority and returns
    all rows of posts and reblogs. priority=[dead|priority] to fetch by either.
    returns
    (O_POST_ID , O_REMOTE_ID, O_ORIGIN_ID, O_REBLOGGED_ID,O_NOTES,O_ORIGIN_ID2,
    O_ORIGIN_NAME,O_REBLOGGED_ID2,O_REBLOGGED_NAME2)"""
    cur = con.cursor()
    # logging.debug(f"{BColors.BLUE}Getting dead blogs posts by priority: {priority}{BColors.ENDC}")
    try:
        cur.execute("select * from FETCH_ALL_BLOG_S_POSTS(?);", (priority,))
        return cur.fetchall()
    except:
        raise
    finally:
        con.commit()


def update_remote_ids_with_notes_count(db, con, rid, name, count):
    """Update all remote_ids with the same notes count"""
    logging.debug(f"Updating all remote_id {rid} with notes count {count}...")
    cur = con.cursor()
    try:
        cur.execute(r'update POSTS set notes = ' + str(count) + r' where remote_id ='
        + str(rid) + r';')
    except BaseException as e:
        traceback.print_exc()
        logging.debug(f'{BColors.FAIL}{name} error while updating rid {rid}: {e}{BColors.ENDC}')
    finally:
        con.commit()


def insert_blogname_gathered(con, name, crawling_status=None):
    """Inserting a gathered blog with crawling status (default null), or
    updating an existing blog with crawling status"""
    # logging.debug(f"insert_blogname_gathered({name})")
    cur = con.cursor()
    try:
        cur.callproc('insert_blogname_gathered', (name, crawling_status))
    except:
        logging.debug(f'{BColors.FAIL}error while inserting {name}\
 crawling_status: {crawling_status}{BColors.ENDC}')
        raise
    finally:
        con.commit()


def update_blog_info(Database, con, blog, ignore_response=False):
    """ updates info if our current values have changed
    compared to what the API gave us last time,
    in case of an update while scraping for example.
    If init, no need for offset and posts_scraped since brand new
    returns dict(last_total_posts, last_updated, last_checked,
    last_offset, last_scraped_posts)"""

    logging.debug(BColors.BLUE + "{0} update DB info. crawl_status: {1} crawling: {2}"\
    .format(blog.name, blog.crawl_status, blog.crawling) + BColors.ENDC)
    cur = con.cursor()
    if blog.crawl_status == 'new':
        # args: (blogname, UP|DEAD|WIPED, total_posts, updated,
        # [crawl_status(resume(default)|dead), crawling(0|1)])
        params = [blog.name,
                blog.health,
                blog.total_posts,
                blog.last_updated,
                blog.crawl_status,
                blog.crawling]
        statmt = 'execute procedure update_blog_info_init(?,?,?,?,?,?);'
    else:
        # args: (blogname, UP|DEAD|WIPED, total_posts, last_updated, current_offset,
        # scraped_so_far, [crawl_status(resume(default)|dead), crawling(0|1)])
        params = [blog.name,
                blog.health,
                blog.total_posts,
                blog.last_updated,
                blog.offset,
                blog.posts_scraped,
                blog.crawl_status,
                blog.crawling]
        statmt = 'execute procedure update_blog_info(?,?,?,?,?,?,?,?);'

    cur.execute(statmt, params)

    if ignore_response or blog.health == 'DEAD': # we don't care about return values
        con.commit()
        return

    resp_dict = defaultdict(int)
    resp_dict['last_health'],\
    resp_dict['last_total_posts'],\
    resp_dict['last_updated'],\
    resp_dict['last_checked'],\
    resp_dict['last_offset'],\
    resp_dict['last_scraped_posts'] = cur.fetchall()[0]
    # logging.debug(BColors.BLUE + "db_resp: {0}, {1}"\
    # .format(type(db_resp), db_resp) + BColors.ENDC)

    con.commit()

    # logging.debug(BColors.BLUE + "resp_dict: {0}"\
    # .format(resp_dict) + BColors.ENDC)
    return resp_dict


def reset_to_brand_new(database, con, blog, reset_type):
    """Resets CRAWL_STATUS varchar field for blog to input reset_type
    called on thread premature cleanup"""
    cur = con.cursor()
    try:
        cur.execute(r'execute procedure reset_crawl_status(?,?);',
                    (blog.name, reset_type))
    except:
        raise
    finally:
        con.commit()


def update_crawling(con, blog=None):
    """ Sets blog crawling status to 0 or 1, if blog=None, reset all to 0.
    Resets any left over 'init' to 'new'"""

    cur = con.cursor()
    if blog is None:
        try:
            logging.warning(f"Resetting 'crawling' in BLOGS table for all")
            # Resets crawling_status from init to new and crawling to 0
            cur.callproc(r"""reset_crawl_status_all""")
        except BaseException as e:
            logging.error(f"Exception in procedure reset_crawl_status_all: {e}")
        finally:
            con.commit()
    else:
        try:
            logging.debug(f"{BColors.BLUE}Resetting crawling_status for '{blog}'{BColors.ENDC}")
            cur.callproc(r"""reset_blog_crawling_status""", (blog,))
        except BaseException as e:
            logging.error(f"Exception in procedure reset_blog_crawling_status: {e}")
        finally:
            con.commit()


def update_or_insert_post(con, update, note_count):
    """Update or insert only one post, used to fix a missing reblogged blogname"""
    cur = con.cursor()
    post = update.posts_response[0]
    if post is None:
        return
    get_post_details(post)

    try:
        cur.callproc('insert_post', (
            post.get('id'),                 # post_id
            post.get('blog_name'),          # blog_name
            post.get('post_url'),           # post_url
            post.get('timestamp'),          # timestamp
            post.get('remote_id'),          # remote_id
            post.get('reblogged_name'),     # reblogged_blog_name
            note_count                      # number or notes is any
            ))
    except BaseException as e:
        logging.error(f"{BColors.FAIL}DB ERROR{BColors.BLUE} post\t{post.get('id')} : {e}{BColors.ENDC}")
    finally:
        con.commit()

    return post.get('reblogged_name')


def insert_posts(database, con, blog, update):
    """ Returns a tuple of number of posts processed, and dupe errors.
    CALL ONLY WITH A LOCK!"""
    cur = con.cursor()
    # t0 = time.time()
    added = 0
    dupes = 0
    errors = 0
    post_errors = 0
    # with fdb.TransactionContext(con):
    for post in update.posts_response: # list of dicts

        try:
            get_post_details(post)
        except:
            con.commit()
            raise

        results = inserted_post(cur, post)
        if not results[0]: # skip the rest because we need post entry in POSTS table
            post_errors += results[1]
            # continue
        dupes += results[2]

        added += 1

        if post.get('notes'): # only if deep-scrape
            scrape_post_notes(cur, post)

        if post.get('content_raw') is not None and instances.my_args.record_context:
            results = inserted_context(cur, post)
            errors += results[1]


        results = inserted_urls(cur, post)
        errors += results[1]

    else: # no problem occured
        logging.debug(BColors.BLUE + "COMMITTING" + BColors.ENDC)
        con.commit()

    # t1 = time.time()
    # logging.debug(f"{BColors.BLUE}Procedures to insert took {1000*(t1-t0):.2f} ms{BColors.ENDC}")

    logging.info(f"{BColors.BLUE}{BColors.BOLD}{blog.name} Successfully \
attempted to insert {added} posts. Failed adding {post_errors} posts. \
{dupes} duplicate posts. \
Failed adding {errors} other items.{BColors.ENDC}")

    update.posts_response = [] #reset

    return added, post_errors, dupes


def get_scraped_post_num(database, con, blog):
    """Queries database for total number of post_id linked to blog.name"""
    cur = con.cursor()
    try:
        cur.execute("select count(*) from (select POST_ID from POSTS where ORIGIN_BLOGNAME =\
 (select auto_id from BLOGS where BLOG_NAME = '" + blog.name + "'));")
        return cur.fetchone()[0]
    finally:
        con.commit()


def get_post_details(post):
    """if there is no content_raw -> get 'reblog' instead (it's the same! but for original post)"""

    full_context = ''
    post['full_context'] = ''
    attr = { # potentially good fields holding context data
        'reblog':                post.get('reblog'),
        'comment':               "",
        'tree_html':             "",
        'body':                  post.get('body'), #FIXME: maybe can default to '' here, not None
        'caption':               post.get('caption'),
        'source_url':            post.get('source_url'),  # type: video, audio
        'answer':                post.get('answer'),    # type: answer
        'question':              post.get('question'),    # type: answer
        'link_url':              post.get('link_url'), # type: photos
        'content_raw':           ''
        }

    trail          = post.get('trail')
    reblogged_name = None
    remote_id      = None

    if attr['reblog'] is not None:
        attr['comment'] = post.get('reblog').get('comment')
        attr['tree_html'] = post.get('reblog').get('tree_html')
        full_context += ' '.join((attr['comment'], attr['tree_html']))

    if attr['body'] is not None:            # type == text
        full_context += ' ' + attr['body']

    if attr['caption'] is not None:         # type in [photo,video]
        full_context += ' ' + attr['caption']

    if attr['source_url'] is not None:      # type in [video, audio, quote]
        full_context += ' ' + attr['source_url']

    if attr['link_url'] is not None:      # type in [video, audio, quote]
        full_context += ' ' + attr['link_url']

    if attr['answer'] is not None:          # type == answer
        full_context += ' '.join(('', attr['question'], attr['answer']))


    if not trail:                       # empty list, there will be no remote_id!
        if post.get('type') == 'text': # text, quote, link, answer, video, audio, photo, chat
            attr['content_raw'] = attr.get('body')
        elif post.get('type') in ['photo', 'video']:
            attr['content_raw'] = attr.get('caption')
        elif post.get('type') == 'answer':
            attr['content_raw'] = ' '.join((attr.get('question'), attr.get('answer')))
        else:
            attr['content_raw'] = ' '.join((attr.get('comment', ''),
            attr.get('tree_html', '')))

        # FIXME: request again from API with param &reblog_info=True
        # if post.get('reblogged_root_id') == post.get('id') and\
        #     post.get('blog_name') == post.get('reblogged_root_name'):
        #     remote_id = post.get('reblogged_root_id')
        #     reblogged_name = post.get('reblogged_root_name')
        # else:
        #     remote_id = post.get('reblogged_from_id')
        #     reblogged_name = post.get('reblogged_from_name')

    if trail:
        found_reblog = False
        for item in trail:
            full_context += ' '.join(('', item.get('content_raw', ''), item.get('content', '')))

            attr['content_raw'] += ' ' + item.get('content_raw')

            if found_reblog:
                continue

            item_remote_id = item.get('post').get('id')
            item_name = item.get('blog').get('name')

            if post.get('id') != item_remote_id:           # we know it's a reblog, not a self reblog, precious
                if item_name != post.get('blog_name') and item_name is not None:    # indeed a foreign reblog
                    if item.get('is_root_item'):            # actual original foreign post
                        found_reblog = True
                        reblogged_name          = item_name
                        remote_id               = item_remote_id

                else:   #same name    # could be self reblog
                    if item.get('is_current_item') and item.get('is_root_item'): #normal post
                        continue

                    if item.get('is_current_item') and not item.get('is_root_item'):     # update / reblog -> update DB context

                        if instances.my_args.record_context:
                            logging.info(f"{BColors.YELLOW}{post.get('blog_name')} \
Would have replaced id {post.get('id')} != rid {item_remote_id} content_raw of length {len(attr.get('content_raw'))} \
with more recently updated version is_current_item: {len(item.get('content_raw'))}\n{attr.get('content_raw')[:1000]}\
\n----------\n{item.get('content_raw')[:1000]}{BColors.ENDC}")
                            # attr['content_raw']     = item.get('content_raw')

                        reblogged_name          = item_name

                    if item.get('is_root_item') and not item.get('is_current_item'):            # just self reblog! normal update name and remote_id, don't update context
                        found_reblog = True
                        reblogged_name          = item_name
                        remote_id               = item_remote_id

            elif post.get('id') == item_remote_id:      # either a self reblog, or just a blog, + blog name is same
                if item_name == post.get('blog_name') and item_name is not None:  # it's just a normal blog, nothing fancy not a reblog but can be part of a reblog (comment added)
                    if item.get('is_current_item'):     # self reblog that was updated! we keep all to update the context explicitly

                        if instances.my_args.record_context:
                            logging.info(f"{BColors.YELLOW}{post.get('blog_name')} \
Replacing {post.get('id')} (id = rid) content_raw of length {len(attr.get('content_raw'))} \
with more recently updated version is_current_item: {len(item.get('content_raw'))}\n\
{attr.get('content_raw')[:1000]}\n----------\n{item.get('content_raw')[:1000]}{BColors.ENDC}")

                            attr['content_raw'] = item.get('content_raw')

                    elif item.get('is_root_item'):      # just self reblog!
                        found_reblog = True
                        reblogged_name          = item_name
                        remote_id               = item_remote_id
                # diff name same id impossible

    # keep the longest field of all
    # stringset = set()
    # for item in set(attr.values()):
    #     stringset.add(value)
    # if trail:
    #     for item in trail:
    #         stringset.add(item.get('content_raw'))
    #         stringset.add(item.get('content'))
    # maxlength = max(len(s) for s in stringset)
    # longest_strings = [s for s in stringset if len(s) == maxlength]
    # attr['content_raw'] = longest_strings[0]

    attr['content_raw'] = attr.get('content_raw', '').replace('\n', '')
    if attr.get('content_raw') == '' or instances.my_args.record_context:
        attr['content_raw'] = None

    if post.get('reblogged_root_id'): # we asked for a deep-scrape
        # logging.warning(f"{BColors.LIGHTPINK}reblogged_root_id found: \
        # {post.get('reblogged_root_id')} {post.get('reblogged_root_name')}{BColors.ENDC}")
        post['remote_id'] = post['reblogged_root_id']
        post['reblogged_name'] = post['reblogged_root_name']
    elif post.get('reblogged_from_id'):
        post['remote_id'] = post['reblogged_from_id']
        post['reblogged_name'] = post['reblogged_from_name']
    else:
        post['remote_id']       = remote_id
        post['reblogged_name']  = reblogged_name


    post['content_raw'] = attr.get('content_raw')
    post['full_context'], post['filtered_urls'] = filter_content_raw(full_context)

#     logging.debug(f"{BColors.CYAN}Added fields to post {post.get('id')}:\nreblogged_name={post.get('reblogged_name')}\n\
# remote_id={post.get('remote_id')}\ncontent_raw={post.get('content_raw')}\nfull_context={post.get('full_context')}\n\
# filtered_urls={post.get('filtered_urls')}\nblogname={post.get('blog_name')}{BColors.ENDC}")

    return post


def filter_content_raw(content, parsehtml=False):
    """Eliminates the html tags, redirects, etc. in contexts
    returns context string and a list of isolated found tumblr urls"""

    if parsehtml:
        content = htmlToText(content)

    urls = extract_urls(content, parsehtml=parsehtml)

    return content, urls


def found_filtered(capped):
    """Completely filter our urls which include strings in filter"""
    for filtered in urls_blacklist_filter:
        if capped.find(filtered) != -1: # redirect
            break
    else:
        return False
    return True


def extract_urls(content, parsehtml=False):
    """Returns a set of unique urls, unquoted, without redirects,
    None if none is found"""

    # found_http_occur = content.count('http')
    # logging.warning("http occurences: {0}".format(found_http_occur))

    url_set = set()
    cache = set()
    # http_walk = 0

    # t0 = time.time()
    for item in http_url_uber_re.findall(content):
        # logging.warning('matched: {0}'.format(item))
        for capped in item:
            if capped in cache or capped is '':
                # http_walk += capped.count('http')
                continue
            if instances.my_args.ignore_non_tumblr_urls and 'tumblr_' not in capped:
                continue
            # logging.warning('captured: {0}'.format(capped))
            cache.add(capped)

            if found_filtered(capped):
                # http_walk += capped.count('http')
                continue

            reresult = repattern_tumblr_redirect.search(capped) # search t.umblr redirects
            if reresult:
                # http_walk += capped.count('http') # we usually find 3 occurences
                capped = reresult.group(1)

            # capped = html.unescape(capped) # remove &amp; -> &
            capped = parse.unquote(capped)         # remove %3A%2F%2F and %20 spaces

            # if capped in url_set:
            #     http_walk += capped.count('http')

            url_set.add(capped)

    # t1 = time.time()
    # logging.debug(BColors.BLUEOK + BColors.BLUE + "URL regexp took %.2f ms" \
    #               % (1000*(t1-t0)) + BColors.ENDC)

    # logging.debug(BColors.BLUE + BColors.BOLD + "url_set length: {0},\n{1}"
    #               .format(len(url_set), url_set) + BColors.ENDC)

    # if len(url_set) < found_http_occur - http_walk:
    #     logging.info(BColors.FAIL + "Warning: less urls than HTTP occurences. {0}<{1}"
    #                 .format(len(url_set), found_http_occur) + BColors.ENDC)
    #     logging.info(BColors.BLUE + "full context was:\n{0}"
    #                 .format(repr(content)) + BColors.ENDC)

    #     singleton = http_url_single_re.search(content)
    #     if singleton:
    #         url_set.add(singleton.group(1))
    #         logging.info(BColors.BLUE +  "Added singleton back: "
    #         + singleton.group(1) + BColors.ENDC)

    #DEBUG: counting all url found
    # global FILTERED_URL_GLOBAL_COUNT
    # for item in url_set:
    #     FILTERED_URL_GLOBAL_COUNT.add(item)
    return url_set



def htmlToText(raw_html):
    """https://stackoverflow.com/questions/14694482/converting-html-to-text-with-python
    WARNING: causes infinite loop in some circumstances? """
    ret = raw_html.replace('\n','').replace('\t','')
    # logging.debug(BColors.BLUE + "RAW html:\n{0}".format(ret) + BColors.ENDC)

    def _getElement(subhtml, name, end=None):
        ename = "<" + name + ">"
        a = subhtml.lower().find(ename)
        if a == -1:
            ename = "<" + name + " "
            a = subhtml.lower().find(ename)
        if a == -1: return
        if end == None: end = "</" + name + ">"
        b = subhtml.lower()[a+len(ename):].find(end)+a+len(end)+len(ename)
        if b-a-len(end)-len(ename) == -1:
            b = subhtml[a+len(ename):].find('>')+a+len('>')+len(ename)
        return subhtml[a:b]

    def _getElementAttribute(element, name):
        a = element.lower().find(name+'="')+len(name+'="')
        if a == -1: return
        b = element[a:].find('"')+a
        return element[a:b]

    def _getElementContent(element):
        a = element.find(">")+len(">")
        if a == -1: return
        b = len(element)-element[::-1].find('<')-1
        return element[a:b]
    # remove scripts
    while True:
        scriptElement = _getElement(ret, 'script')
        if not scriptElement: scriptElement = _getElement(ret, 'script', '</noscript>')
        if not scriptElement: break
        ret = ret.replace(scriptElement, '')
    # replace links
    while True:
        linkElement = _getElement(ret, 'a')
        if not linkElement: break
        linkElementContent = _getElementContent(linkElement)
        if linkElementContent is not None:
            #this will replace: '<a href="some.site">text</a>' -> 'text'
                #   ret = ret.replace(linkElement, linkElementContent)
            #this will replace: '<a href="some.site">link</a>' -> 'some.site'
                #   linkElementHref = _getElementAttribute(linkElement, 'href')
                #   if linkElementHref:
                #       ret = ret.replace(linkElement, linkElementHref)
            #this will replace: '<a href="some.site">link</a>' -> 'text ( some.site )'
            linkElementHref = _getElementAttribute(linkElement, 'href')
            if linkElementHref:
                ret = ret.replace(linkElement, linkElementContent+' ( '+linkElementHref+' )')

    #replace paragraphs
    while True:
        paragraphElement = _getElement(ret, 'p')
        if not paragraphElement: break
        paragraphElementContent = _getElementContent(paragraphElement)
        if paragraphElementContent:
            ret = ret.replace(paragraphElement, '\n'+paragraphElementContent+'\n')
        else:
            ret = ret.replace(paragraphElement, '')

    #replace line breaks
    ret = ret.replace('<br>', '\n')
    ret = ret.replace('<br/>', '\n')

    #replace images
    while True:
        imgElement = _getElement(ret, 'img')
        if not imgElement: break
        imgElementSrc = _getElementAttribute(imgElement, 'src')
        if imgElementSrc:
            ret = ret.replace(imgElement, '[IMG] '+imgElementSrc+' [IMG]')
        else:
            ret = ret.replace(imgElement, '')
    #remove rest elements
    while True:
        a = ret.find("<")
        if a == -1: break
        b = ret[a:].find(">")+a
        if b-a == -1: break
        b2 = ret[b:].find(">")+b
        if b2-b == -1: break
        element = _getElement(ret, ret[a+1:b2])
        if element:
            elementContent = _getElementContent(element)
            if elementContent:
                ret = ret.replace(element, elementContent)
            else:
                ret = ret.replace(element, '')
    logging.debug(BColors.LIGHTPINK + "PARSED html:\n{0}".format(repr(ret)) + BColors.ENDC)
    return ret


def inserted_post(cur, post):
    """Returns True to ignore error"""
    # cur = con.cursor()
    errors = 0
    dupes = 0
    success = True
    note_count = None
    if post.get('notes'):
        note_count = len(post.get('notes'))
    try:
        cur.callproc('insert_post', (
            post.get('id'),                 # post_id
            post.get('blog_name'),          # blog_name
            post.get('post_url'),           # post_url
            post.get('timestamp'),          # timestamp
            post.get('remote_id'),          # remote_id
            post.get('reblogged_name'),     # reblogged_blog_name
            note_count                      # number or notes is any
            ))
    except fdb.DatabaseError as e:
        if str(e).find("violation of PRIMARY or UNIQUE KEY constraint") != -1:
            logging.error(f"{BColors.FAIL}DB ERROR{BColors.BLUE} post\t\
{post.get('id')} : duplicate{BColors.ENDC}")
            dupes += 1
            success = True
        else:
            logging.error(f"{BColors.FAIL}{BColors.BOLD}DB ERROR{BColors.BLUE} post\t\
{post.get('id')} : {e}{BColors.ENDC}")
            errors += 1
            success = False
    except BaseException as e:
        logging.debug(f"{BColors.FAIL}ERROR post\t{post.get('id')} : {e}{BColors.ENDC}")
        errors += 1
        success = False

    # con.commit()
    return success, errors, dupes


def scrape_post_notes(cur, post):
    """Get blognames from notes and insert them into BLOGS table"""

    for note in post.get('notes'):
        if note.get('post_id') is not None: # only when "type": "reblog"
            # logging.debug(f"{BColors.LIGHTPINK}Note: {note.get('post_id')} {note.get('blog_name')}{BColors.ENDC}")
            try:
                cur.callproc('insert_post', (
                    note.get('post_id'),                                # post_id
                    note.get('blog_name'),                              # blog_name (not null)
                    note.get('blog_url') + "post/" + note.get('post_id'),# post_url (not null)
                    note.get('timestamp'),                              # timestamp
                    post.get('remote_id'),                              # remote_id
                    post.get('reblogged_name')                          # reblogged_blog_name
                    ))
            except fdb.DatabaseError as e:
                # if str(e).find("violation of PRIMARY or UNIQUE KEY constraint") != -1:
                #     e = "duplicate"
                logging.info(f"{BColors.FAIL}DB ERROR{BColors.BLUE} \
post note\t{post.get('id')}: {e}{BColors.ENDC}")
            except BaseException as e:
                logging.debug(f"{BColors.FAIL}ERROR post note\t\
{post.get('id')} : {e}{BColors.ENDC}")


        elif note.get('blog_name') is not None: # otherwise, just a "type": "like"
            try:
                cur.callproc('insert_blogname_gathered',
                            (note.get('blog_name'), 'new'))
            except BaseException as e:
                logging.debug(f"{BColors.FAIL}error while inserting \
{note.get('blog_name')} crawling_status: new. {e}{BColors.ENDC}")


def inserted_context(cur, post):
    # cur = con.cursor()
    errors = 0
    success = True
    try:
        # logging.warning(BColors.BOLD + "call to insert_context for {0} ({1},{2},{3},{4})".format(
        #             post.get('blog_name'),
        #             post.get('id'),
        #             post.get('timestamp'),         #timestamp
        #             post.get('remote_id'),         #remote_id
        #             post.get('content_raw', None)  #remote_content
        #             ) + BColors.ENDC)

        cur.callproc('insert_context', (
                    post.get('id'),
                    post.get('timestamp'),         #timestamp
                    post.get('remote_id'),         #remote_id
                    post.get('content_raw')        #remote_content
                    ))
        # logging.warning("context returns: {0} ".format(repr(cur.fetchall())))
    except fdb.DatabaseError as e:
        # if str(e).find("violation of PRIMARY or UNIQUE KEY constraint") != -1:
        #     e = "duplicate"
        logging.debug(BColors.FAIL + "DB ERROR" + BColors.BLUE
        + " context\t{0} : {1}".format(post.get('id'), e) + BColors.ENDC)
        errors += 1
        success = True
    except BaseException as e:
        logging.critical(BColors.FAIL + "ERROR" + " context\t{0}: {1}"
        .format(post.get('id'), e) + BColors.ENDC)
        errors += 1
        success = False
        if str(e).find('is too long, expected') != -1:
            toolong = True
            maxsize = 32800
            attempt = 0
            while toolong: #unicode(?) is big, need to trim until it fits
                attempt += 1
                if attempt >= 320:
                    break
                maxsize -= 100
                try:
                    cur.callproc('insert_context',
                    (post.get('id'), post.get('timestamp'),
                    post.get('remote_id'), post.get('content_raw')[maxsize]))
                    toolong = False
                except:
                    continue
            else:
                logging.critical(BColors.BLUEOK +
                "Instead, inserted trimmed context after {0} attempts: {1}[...]"
                .format(attempt, post.get('content_raw')[:1000]))
                errors -= 1
                success = True
            if toolong:
                logging.debug("{9}Failed inserting trimmed context for {1}: {2}{3}"
                .format(BColors.FAIL, post.get('id'), post.get('content_raw')[:1000],
                BColors.ENDC))


    # con.commit()
    return success, errors


def inserted_urls(cur, post):
    # cur = con.cursor()
    errors = 0
    photos = post.get('photos')
    if photos is not None:
        # insertstmt = cur.prep('insert into URLS (file_url,post_id,remote_id) values (?,?,?);')
        for photo in photos:
            # logging.debug("inserting normal url:{0}".format(photo.get('original_size').get('url')))
            # DEBUG
            # global FILTERED_URL_GLOBAL_COUNT
            # FILTERED_URL_GLOBAL_COUNT.add(photo.get('original_size').get('url'))
            try:
                cur.callproc('insert_url', (
                            photo.get('original_size').get('url'),
                            post.get('id'),
                            post.get('remote_id')
                            ))
            except fdb.DatabaseError as e:
                # if str(e).find("violation of PRIMARY or UNIQUE KEY constraint") != -1:
                #     e = "duplicate"
                logging.debug(f"{BColors.FAIL}DB ERROR{BColors.BLUE} url\t\
{photo.get('original_size').get('url')} : {e}{BColors.ENDC}")
                errors += 1
                continue
    # con.commit()
    # cur = con.cursor()
    for url in post.get('filtered_urls'):
        # logging.debug("inserting filtered url:{0}".format(url))
        try:
            cur.callproc('insert_url', (
                        url,
                        post.get('id'),
                        post.get('remote_id')
                        ))
        except fdb.DatabaseError as e:
            # if str(e).find("violation of PRIMARY or UNIQUE KEY constraint") != -1:
            #     e = "duplicate"
            logging.debug(BColors.FAIL + "DB ERROR" + BColors.BLUE
            + " url\t{0} : {1}".format(
            url, e) + BColors.ENDC)
            errors += 1
            continue
        except BaseException as e:
            logging.critical("{0}Error inserting url {1}. {2}{3}"
            .format(BColors.FAIL, url, repr(e), BColors.ENDC)) #exc_info=True)
            errors += 1
            if str(e).find('is too long, expected') != -1:
                try:
                    url = parse.unquote(url)
                    cur.callproc('insert_url', (url[:999], post.get('id'), post.get('remote_id')))
                except:
                    continue
                logging.critical(BColors.BLUEOK + "Instead, inserted trimmed url {0}."
                .format(url[:999]))
                errors -= 1
            continue

    # con.commit()

    return True, errors


def look_for_lost_urls(con, write_path):
    """Warning: Very long compute. Fetch all URLS which point to a file's base hash
    listed in the 1280 archive table and write a csv to the write_path"""
    cur = con.cursor()
    logging.warning("Getting lost urls")
    counter = 0
    t0 = time.time()
    with open(write_path + os.sep + "found_urls_in_db" + time.strftime('%Y%m%d_%H%M%S'), 'w') as f:
        try:
            cur.execute(r"""select a.filebasename, b.file_url from OLD_1280 a join urls b on b.FILE_URL like '%'||a.FILEBASENAME||'%'""")
            while True:
                results = cur.fetchmany()
                if not results:
                    break
                counter += 1
                for filebasename, url in results:
                    f.write(f'{filebasename}\t{url}\n')

        except BaseException as e:
            print(f'Exception occured {e}')
            pass
        finally:
            con.rollback()
    logging.warning(f"{BColors.BLUE}Found {counter} urls in {(time.time() - t0):.2f} sec.{BColors.ENDC}")


if __name__ == "__main__":
    import tumblrmapper
    SCRIPTDIR = os.path.dirname(__file__)
    args = tumblrmapper.parse_args()
    tumblrmapper.setup_config(args)

    blogs_toscrape = SCRIPTDIR + os.sep + "tools/blogs_toscrape_test.txt"
    archives_toload = SCRIPTDIR + os.sep +  "tools/1280_files_list.txt"
    found_urls_in_db_path = SCRIPTDIR + os.sep + "tools"
    database = Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                            + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))

    # os.nice(20)
    con = database.connect()
    # create_blank_database(database)
    # populate_db_with_blogs(database, blogs_toscrape)
    # Optional archives too
    # populate_db_with_archives(database, archives_toload)
    look_for_lost_urls(con, found_urls_in_db_path)

    # print('FILTERED_URL_GLOBAL_COUNT: {0}'.format(len(FILTERED_URL_GLOBAL_COUNT)))
