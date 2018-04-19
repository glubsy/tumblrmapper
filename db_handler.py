#!/bin/env python
import csv
import json
import os
import re
import sys
import time
import traceback
import logging
# import html.parser
from urllib import parse
from html.parser import HTMLParser
from operator import itemgetter
import fdb
# import cProfile
import tumblrmapper
from constants import BColors

SCRIPTDIR = os.path.dirname(__file__) + os.sep

http_url_simple_re = re.compile(r'"(https?(?::\/\/|%3A%2F%2F).*?)"', re.I)
http_url_single_re = re.compile(r'(https?(?::\/\/|%3A%2F%2F).*?)(?:\s)*?$', re.I)
# matches quoted, between quotes, before html tags
http_url_super_re = re.compile(r'(?:\"(https?(?::\/\/|%3A%2F%2F).*?)(?:\")(?:<\/)*?)|(?:(https?:\/\/.*?)(?:(?:\s)|(?:<)))', re.I)
repattern_tumblr_redirect = re.compile(r't\.umblr\.com\/redirect\?z=(.*)(&|&amp;)t=.*', re.I)

htmlparser = HTMLParser()

class Database():
    """handle the db file itself, creating everything
    Args are: filepath, user, password, bloglist, archives, host=None"""


    def __init__(self, *args, **kwargs):
        """initialize with environment"""
        self.host = kwargs.get('db_host', 'localhost') #not implemented
        self.db_filepath = kwargs.get('db_filepath', SCRIPTDIR + "blank_db.fdb")
        self.username = kwargs.get('username', "sysdba")
        self.password = kwargs.get('password', "masterkey")
        self.con = []


    def __exit__(self, exc_type, exc_val, exc_tb):
        """ close all remaining connections"""
        logging.debug(BColors.BLUE + "Closing connections to DB" + BColors.ENDC)
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
            for item in self.con:
                item.close()
                self.con.remove(item)
        self.con.remove(con)
        return con.close()



def create_blank_db_file(database):
    """creates the db at host"""

    # ("create database 'host:/temp/db.db' user 'sysdba' password 'pass'")
    c = r"create database " + r"'" + database.db_filepath + \
    r"' user '" + database.username + r"' password '" + database.password + r"'"
    fdb.create_database(c)



def create_blank_database(database):
    """Creates a new blank DB file and populates with tables"""
    create_blank_db_file(database)
    populate_db_with_tables(database)


def populate_db_with_tables(database):
    """Create our tables and procedures here in the DB"""
    con = fdb.connect(database=database.db_filepath, \
                      user=database.username, password=database.password)
    with fdb.TransactionContext(con):
        #auto rollback if exception is raised, and no need to con.close() because automatic
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
        con.execute_immediate(\
"""
CREATE TABLE BLOGS (
AUTO_ID         D_AUTO_ID PRIMARY KEY,
BLOG_NAME       D_BLOG_NAME,
HEALTH          varchar(5),
TOTAL_POSTS     INTEGER,
CRAWL_STATUS    varchar(10) DEFAULT 'new',
CRAWLING        D_BOOLEAN default 0,
POST_OFFSET     INTEGER,
POSTS_SCRAPED   INTEGER,
LAST_CHECKED    D_EPOCH,
LAST_UPDATE     D_EPOCH,
PRIORITY        smallint,
CONSTRAINT blognames_unique UNIQUE (BLOG_NAME) using index ix_blognames
);""")

        con.execute_immediate(\
            """CREATE TABLE GATHERED_BLOGS (
            AUTO_ID      D_AUTO_ID PRIMARY KEY,
            BLOG_NAME    D_BLOG_NAME );""")
            #TODO: make constraint CHECK to only create if not already in tBLOGS?

        con.execute_immediate(\
"""
CREATE TABLE POSTS (
POST_ID             D_POST_NO PRIMARY KEY,
REMOTE_ID           D_POST_NO,
ORIGIN_BLOGNAME     D_AUTO_ID NOT NULL,
REBLOGGED_BLOGNAME  D_AUTO_ID,
POST_URL            D_POSTURL NOT NULL,
POST_DATE           varchar(26),
FOREIGN KEY(ORIGIN_BLOGNAME) REFERENCES BLOGS(AUTO_ID),
FOREIGN KEY(REBLOGGED_BLOGNAME) REFERENCES GATHERED_BLOGS(AUTO_ID)
);""")
            #BLOG_ORIGIN is the blog of the post_id (TID)
            #BLOG_REBLOGGED is name of blog in trail

        con.execute_immediate(\
"""
CREATE TABLE CONTEXTS (
POST_ID         D_POST_NO,
REMOTE_ID       D_POST_NO UNIQUE,
TTIMESTAMP      D_EPOCH,
CONTEXT         D_SUPER_LONG_TEXT,
LATEST_REBLOG   D_POST_NO,
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
            ); """)
            # if remote_id is null, it means it was not a reblog
            # if it's not null, it's from a reblog,

        con.execute_immediate(
            "CREATE TABLE OLD_1280 ( FILENAME varchar(60), FILEBASENAME varchar(60) );")

        # CREATE generators and triggers
        con.execute_immediate("CREATE SEQUENCE tBLOGS_autoid_sequence;")
        con.execute_immediate("CREATE SEQUENCE tGATHERED_BLOGS_autoid_sequence;")
        con.execute_immediate(\
            """CREATE TRIGGER tGATHERED_BLOGS_AUTOINC FOR GATHERED_BLOGS
            ACTIVE BEFORE INSERT POSITION 0
            AS BEGIN NEW.AUTO_ID = next value for tGATHERED_BLOGS_autoid_sequence; END""")

        # CREATE procedures
        # Records given blogname into BLOG table, increments auto_id,
        # decrements auto_id in case an exception occured (on non unique inputs)
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE insert_blogname
( i_blogname d_blog_name, i_prio smallint default null )
AS declare variable v_generated_auto_id d_auto_id;
BEGIN
v_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence, 1);
INSERT into BLOGS (AUTO_ID, BLOG_NAME, PRIORITY) values (:v_generated_auto_id, :i_blogname, :i_prio);
WHEN ANY
DO
v_generated_auto_id = GEN_ID(tBLOGS_autoid_sequence, -1);
END
""")

        # Inserts a post and all its metadata
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE insert_post
(   i_postid d_post_no,
    i_blog_origin d_blog_name,
    i_post_url d_posturl,
    i_post_date varchar(26),
    i_remoteid d_post_no default null,
    i_reblogged_blog_name d_blog_name default null
)
AS declare variable v_blog_origin_id d_auto_id;
declare variable v_fetched_reblogged_blog_id d_auto_id default null;
declare variable v_b_update_gathered d_boolean default 0;
BEGIN
select AUTO_ID from BLOGS where BLOG_NAME = :i_blog_origin into :v_blog_origin_id;

if (:i_reblogged_blog_name is not null) THEN
select AUTO_ID from GATHERED_BLOGS where BLOG_NAME = :i_reblogged_blog_name into :v_fetched_reblogged_blog_id;

if ((:i_reblogged_blog_name is distinct from :i_blog_origin) and (:v_fetched_reblogged_blog_id is null))
THEN v_b_update_gathered = 1;

if ((v_b_update_gathered = 1) and (:i_reblogged_blog_name is not null)) THEN
INSERT into GATHERED_BLOGS (BLOG_NAME) values (:i_reblogged_blog_name)
returning (AUTO_ID) into :v_fetched_reblogged_blog_id;

INSERT into POSTS (POST_ID, POST_URL, POST_DATE, REMOTE_ID,
ORIGIN_BLOGNAME, REBLOGGED_BLOGNAME)
values (:i_postid, :i_post_url, :i_post_date, :i_remoteid,
:v_blog_origin_id, :v_fetched_reblogged_blog_id);
END
""")

        # Inserts context, update if already present with latest reblog's values
        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE insert_context(
    i_post_id d_post_no not null,
    i_timestamp d_epoch,
    i_context d_super_long_text default null,
    i_remote_id d_post_no default null)
as
BEGIN
if (:i_remote_id is not null) then /* we might not want to keep it*/
    if (exists (select (REMOTE_ID) from CONTEXTS where (REMOTE_ID = :i_remote_id))) then
    begin
        if (:i_timestamp > (select (TTIMESTAMP) from CONTEXTS where (REMOTE_ID = :i_remote_id))) then
            update CONTEXTS set CONTEXT = :i_context,
            TTIMESTAMP = :i_timestamp,
            LATEST_REBLOG = :i_post_id
            where (REMOTE_ID = :i_remote_id);
            exit;
    end
else /* we store everything, it's an original post*/
insert into CONTEXTS (POST_ID, TTIMESTAMP, CONTEXT, REMOTE_ID ) values
                    (:i_post_id, :i_timestamp, :i_context, :i_remote_id );
END
""")


        con.execute_immediate(\
"""
CREATE OR ALTER PROCEDURE FETCH_ONE_BLOGNAME
RETURNS (
    O_NAME D_BLOG_NAME,
    O_OFFSET INTEGER,
    O_HEALTH VARCHAR(5),
    O_STATUS VARCHAR(10),
    O_TOTAL INTEGER,
    O_SCRAPED INTEGER,
    O_CHECKED D_EPOCH,
    O_UPDATED D_EPOCH)
AS
BEGIN
if (exists (select (BLOG_NAME) from BLOGS where ((CRAWL_STATUS = 'resume') and (CRAWLING != 1)))) then begin
    for
    select BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED, LAST_UPDATE
    from BLOGS where ((CRAWL_STATUS = 'resume') and (CRAWLING != 1)) order by PRIORITY desc nulls last ROWS 1
    with lock
    into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked, :o_updated
    as cursor cur do
        update BLOGS set CRAWLING = 1 where current of cur;
    suspend;
    end
else
if (exists (select (BLOG_NAME) from BLOGS where (CRAWL_STATUS = 'new'))) then begin
    for select BLOG_NAME, HEALTH, TOTAL_POSTS, CRAWL_STATUS, POST_OFFSET, POSTS_SCRAPED, LAST_CHECKED, LAST_UPDATE
            from BLOGS where (CRAWL_STATUS = 'new')
        order by PRIORITY desc nulls last ROWS 1 with lock
        into :o_name, :o_health, :o_total, :o_status, :o_offset, :o_scraped, :o_checked, :o_updated as cursor tcur do
        update BLOGS set CRAWL_STATUS = 'init' where current of tcur;
    suspend;
    end
END
""")

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
END""")


        # called when quitting script, or done scaping total_posts
        con.execute_immediate(\
"""CREATE OR ALTER PROCEDURE update_crawling_blog_status (i_name d_blog_name, i_input d_boolean)
AS BEGIN
update BLOGS set CRAWLING = 0 where BLOG_NAME = :i_name;
END""")

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


def populate_db_with_archives(database, archivepath):
    """read archive list and populate the OLD_1280 table"""
    con = fdb.connect(database=database.db_filepath, \
    user=database.username, password=database.password)

    cur = con.cursor()
    oldfiles = readlines(archivepath)

    repattern_tumblr = re.compile(r'(tumblr_.*)_.*\..*', re.I) #eliminate '_resol.ext'
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
    logging.debug(BColors.BLUE + "Inserting records into OLD_1280 Took %.2f ms"
                  % (1000*(t1-t0)) + BColors.ENDC)


def populate_db_with_blogs(database, blogpath):
    """read csv list or blog, priority and insert them into BLOGS table """
    con = fdb.connect(database=database.db_filepath,
                      user=database.username, password=database.password)
    cur = con.cursor()
    t0 = time.time()
    with fdb.TransactionContext(con):
        insert_statement = cur.prep("execute procedure insert_blogname(?,?)")

        for blog, priority in read_csv_bloglist(blogpath):
            params = (blog.rstrip() , priority)
            try:
                cur.execute(insert_statement, params)
            except fdb.fbcore.DatabaseError as e:
                if "violation of PRIMARY or UNIQUE KEY" in e.__str__():
                    logging.info(BColors.FAIL + "Error" + BColors.BLUE
                    + " inserting {0}: duplicate.".format(blog) + BColors.ENDC)
        con.commit()

    t1 = time.time()
    logging.debug(BColors.BLUE + 'Inserting records into BLOGS Took %.2f ms'
                  % (1000*(t1-t0)) + BColors.ENDC)


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


def fetch_random_blog(database, con):
    """ Queries DB for a blog that is available
    returns: name, offset, health, status, total posts,
    scraped posts, last checked, last updated
    """
    cur = con.cursor()
    with fdb.TransactionContext(con):
        # sql = cur.prep("execute procedure fetch_one_blogname;")
        cur.execute("execute procedure fetch_one_blogname;")
        # cur.execute("select * from blogs;")
        return cur.fetchone()
        #tuple ('blog', None, None, 'new', None, None, None, None, None)


def update_blog_info(Database, con, blog, init=False, end=False):
    """ updates info if our current values have changed
    compared to what the API gave us last time,
    in case of an update while scraping for example.
    If init, no need for offset and posts_scraped since brand new
    returns dict(last_total_posts, last_updated, last_checked,
    last_offset, last_scraped_posts)"""

    logging.debug(BColors.BLUE + "{0} update DB info. init={1} end={2}"\
    .format(blog.name, init, end) + BColors.ENDC)
    cur = con.cursor()
    if init:
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

    if end: # we don't care about return values
        con.commit()
        return

    db_resp = cur.fetchall()[0]
    # logging.debug(BColors.BLUE + "db_resp: {0}, {1}"\
    # .format(type(db_resp), db_resp) + BColors.ENDC)

    con.commit()

    resp_dict = {}
    resp_dict['last_health'],
    resp_dict['last_total_posts'],
    resp_dict['last_updated'],
    resp_dict['last_checked'],
    resp_dict['last_offset'],
    resp_dict['last_scraped_posts'] = db_resp
    # logging.debug(BColors.BLUE + "resp_dict: {0}"\
    # .format(resp_dict) + BColors.ENDC)
    return resp_dict


def reset_to_brand_new(database, con, blog):
    cur = con.cursor()
    cur.execute(r"update BLOGS set CRAWL_STATUS = 'new' where BLOG_NAME = (?);",
                (blog.name,))
    con.commit()


def update_crawling(database, con, blog=None):
    """ Sets blog crawling status to 0 or 1, if blog=None, reset all to 0"""

    cur = con.cursor()
    if not blog:
        cur.execute('update BLOGS set CRAWLING = 0;')
        logging.debug(BColors.BLUEOK + BColors.BLUE
        + "Reset crawling for all" + BColors.ENDC)
    else:
        cur.execute('execute procedure update_crawling_blog_status(?,?);',
                    (blog.name, blog.crawling))
        logging.debug(BColors.BLUEOK + BColors.BLUE
        + "{0} set crawling to {1}".format(blog.name, blog.crawling) + BColors.ENDC)
    con.commit()


def get_update_response_items(update):
    """ DEPRECATED """
    for post in update.posts_response: #dict in list
        current_post_dict = {}
        current_post_dict['id'] = post.get('id')
        current_post_dict['date'] = post.get('date')
        current_post_dict['updated'] = post.get('updated')
        current_post_dict['post_url'] = post.get('post_url')
        current_post_dict['blog_name'] = post.get('blog_name')
        current_post_dict['timestamp'] = post.get('timestamp')
        current_post_dict['current_context'] = post.get('reblog').get('')

        if 'trail' in post.keys() and len(post['trail']) > 0: # trail is not empty, it's a reblog
            #FIXME: put this in a trail subdictionary
            current_post_dict['trail'] = []
            for item in post.get('trail'):
                current_post_dict['trail'].append({})
                current_post_dict['trail'][item]['reblogged_blog_name'] = post['trail'][item]['blog']['name']
                current_post_dict['trail'][item]['remote_id'] = int(post['trail'][item]['post']['id'])
                current_post_dict['trail'][item]['remote_content'] = post['trail'][item]['content_raw'].replace('\n', '')

        else: #trail is an empty list
            current_post_dict['reblogged_blog_name'] = None
            current_post_dict['remote_id'] = None
            current_post_dict['remote_content'] = None
            pass #FIXME: maybe problematic for the following, might get skipped

        current_post_dict['photos'] = []
        if 'photos' in post.keys():
            for item in range(0, len(post['photos'])):
                current_post_dict['photos'].append(post['photos'][item]['original_size']['url'])

        # update.trimmed_posts_list.append(current_post_dict)



def insert_posts(database, con, blog, update):
    """ Returns the number of posts processed"""
    cur = con.cursor()
    t0 = time.time()
    added = 0
    errors = 0
    with fdb.TransactionContext(con):
        for post in update.posts_response: # list of dicts

            get_remote_id_and_context(post)

            results = inserted_post(cur, post)
            if not results[0]:
                errors += results[1]
                continue
            added += 1

            # results = inserted_context(cur, post)
            # errors += results[1]

            # results = inserted_urls(cur, post)
            # errors += results[1]
        else:
            logging.debug(BColors.BLUE + "COMMITTING" + BColors.ENDC)
            con.commit()

    t1 = time.time()
    logging.debug(BColors.BLUE + "Procedures to insert took %.2f ms" \
                    % (1000*(t1-t0)) + BColors.ENDC)

    logging.info(BColors.BLUE + "{0} Successfully added {1} posts."\
    .format(blog.name, added) + BColors.ENDC)
    logging.info(BColors.BLUE + "{0} Failed adding {1} posts."\
    .format(blog.name, errors) + BColors.ENDC)
    update.posts_response = [] #reset
    return added



def get_remote_id_and_context(post):
    """if there is no 'content_raw'
    ---> get 'reblog' instead (it's the same! but for original post)"""

    full_context = ''
    post['full_context'] = ''
    attr = { # potentially good fields holding context data
        'reblog':                post.get('reblog'),
        'comment':               None,
        'tree_html':             None,
        'body':                  post.get('body'),
        'caption':               post.get('caption'),
        'source_url':            post.get('source_url'),  # type == video, audio
        'answer':                post.get('answer'),    # type == answer
        'content_raw':           ''}

    trail          = post.get('trail')
    reblogged_name = None
    remote_id      = None

    if attr['reblog'] is not None:
        attr['comment'] = post.get('reblog').get('comment')
        attr['tree_html'] = post.get('reblog').get('tree_html')
        full_context += attr['comment'] + attr['tree_html']

    if attr['body'] is not None:            # type == text
        full_context += attr['body']

    if attr['caption'] is not None:         # type in [photo,video]
        full_context += attr['caption']

    if attr['source_url'] is not None:      # type in [video, audio, quote]
        full_context += attr['source_url']

    if attr['answer'] is not None:          # type == answer
        full_context += attr['answer']

    if trail:
        for item in trail:
            full_context += item.get('content_raw', '') + item.get('content', '')

            attr['content_raw'] += item.get('content_raw')

            item_remote_id = item.get('post').get('id')
            if post.get('id') != item_remote_id:            # not a self reblog, precious
                if item.get('is_root_item'):                # original post
                    reblogged_name          = item.get('blog').get('name')
                    remote_id               = item_remote_id

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

    if not trail:                       # empty list, there will be no remote_id!
        if post.get('type') == 'text': # text, quote, link, answer, video, audio, photo, chat
            attr['content_raw'] = attr.get('body')
        elif post.get('type') in ['photo', 'video']:
            attr['content_raw'] = attr.get('caption')
        elif post.get('type') == 'answer':
            attr['content_raw'] = attr.get('answer')
        else:
            attr['content_raw'] = post.get('reblog').get('comment', '')\
            + post.get('reblog').get('tree_html', '')


    post['reblogged_name']  = reblogged_name
    post['remote_id']       = remote_id
    post['content_raw']     = attr.get('content_raw')
    post['full_context'],
    post['filtered_urls']   = filter_content_raw(full_context)

    return post




def filter_content_raw(content, parsehtml=False):
    """Eliminates the html tags, redirects, etc. in contexts
    returns context string and a list of isolated found tumblr urls"""

    if parsehtml:
        content = htmlToText(content)

    urls = extract_urls(content, parsehtml=parsehtml)

    return content, urls


def extract_urls(content, parsehtml=False):
    """Returns a set of unique urls, unquoted, without redirects,
    None if none is found"""

    found_http_occur = content.count('http')
    logging.debug("http occurences: {0}".format(found_http_occur))

    if not found_http_occur:
        return

    url_set = set()
    checked = set()
    http_walk = 0
    t0 = time.time()
    for item in http_url_super_re.findall(content):
        # logging.debug('matched: {0}'.format(item))
        for capped in item:
            if capped in checked or capped is '':
                http_walk += capped.count('http')
                continue
            logging.debug('captured: {0}'.format(capped))
            checked.add(capped)

            if capped.find("://tmblr.co/") != -1:
                http_walk += capped.count('http')
                continue

            reresult = repattern_tumblr_redirect.search(capped) # search for t.umblr
            if reresult:
                http_walk += capped.count('http') # we usually find 3 occurences
                capped = reresult.group(1)

            # capped = htmlparser.unescape(capped) # remove &amp;
            capped = parse.unquote(capped)         # remove %3A%2F%2F and %20 spaces

            if capped in url_set:
                http_walk += capped.count('http')

            url_set.add(capped)

    t1 = time.time()
    logging.debug(BColors.BLUEOK + BColors.BLUE + "Procedures to insert took %.2f ms" \
                  % (1000*(t1-t0)) + BColors.ENDC)

    logging.debug(BColors.BLUE + BColors.BOLD + "url_set length: {0},\n{1}"
                  .format(len(url_set), url_set) + BColors.ENDC)

    if len(url_set) < found_http_occur - http_walk:
        logging.info(BColors.FAIL + "Warning: less urls than HTTP occurences. {0}<{1}"
                      .format(len(url_set), found_http_occur) + BColors.ENDC)
        logging.info(BColors.BLUE + "full context was:\n{0}".format(repr(content)) + BColors.ENDC)

        singleton = http_url_single_re.search(content)
        if singleton:
            url_set.add(singleton.group(1))
            logging.debug("Added singleton: " + singleton.group(1) )

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

    errors = 0
    success = True
    try:
        cur.callproc('insert_post', (\
                post.get('id'),                 # post_id
                post.get('blog_name'),          # blog_name
                post.get('post_url'),           # post_url
                post.get('date'),               # timestamp
                post.get('remote_id'),          # remote_id
                post.get('reblogged_name')      # reblogged_blog_name
                ))
    except fdb.DatabaseError as e:
        if str(e).find("violation of PRIMARY or UNIQUE KEY constraint"):
            e = "duplicate"
        logging.info(BColors.FAIL + "DB ERROR" + BColors.BLUE + \
        " post\t{0}: {1}".format(post.get('id'), e) + BColors.ENDC)
        success = True
    except Exception as e:
        logging.info(BColors.FAIL + "ERROR" + BColors.BLUE + \
        " post\t{0}: {1}".format(post.get('id'), e) + BColors.ENDC)
        success = False
    return success, errors


def inserted_context(cur, post):
    errors = 0
    success = True
    try:
        cur.callproc('insert_context', (\
                    post.get('id'),\
                    post.get('timestamp'),          #timestamp
                    post.get('content_raw', None),  #remote_content
                    post.get('remote_id')           #remote_id
                    ))
    except fdb.DatabaseError as e:
        if str(e).find("violation of PRIMARY or UNIQUE KEY constraint"):
            e = "duplicate"
        logging.info(BColors.FAIL + "DB ERROR" + BColors.BLUE
        + " context\t{0}: {1}".format(post.get('id'), e) + BColors.ENDC)
        success = True
    except Exception as e:
        logging.info(BColors.FAIL + "ERROR" + BColors.BLUE
        + " context\t{0}: {1}".format(post.get('id'), e) + BColors.ENDC)
        success = False
    return success, errors


def inserted_urls(cur, post):

    errors = 0
    photos = post.get('photos', False)
    if not photos:
        return True, errors

    insertstmt = cur.prep('insert into URLS (file_url,post_id,remote_id) values (?,?,?);')
    for photo in photos:
        # logging.debug("photo: {0} {1} {2}"
        # .format(photo, post.get('id'), post.get('remote_id')))
        try:
            cur.execute(insertstmt, (
                        photo.get('original_size').get('url'),
                        post.get('id'),
                        post.get('remote_id')
                        ))
        except fdb.DatabaseError as e:
            if str(e).find("violation of PRIMARY or UNIQUE KEY constraint"):
                e = "duplicate"
            logging.info(BColors.FAIL + "DB ERROR" + BColors.BLUE
                        + " url\t{0}: {1}".format(
                        photo.get('original_size').get('url'),
                        e) + BColors.ENDC)
            continue
    return True, errors



# DEBUG
def unittest_update_table(db, con, payload):
    """feed testing data"""
    json = payload.get('response')
    # con = fdb.connect(database=database.db_filepath,
    # user=database.username, password=database.password)

    blog = tumblrmapper.TumblrBlog()
    blog.name = json.get('blog').get('name')

    update = tumblrmapper.UpdatePayload()
    update.posts_response = json.get('posts') #list
    cur = con.cursor()
    cur.execute('execute procedure insert_blogname(?,?)', (blog.name, 1))
    con.commit()
    return insert_posts(db, con, blog, update)


if __name__ == "__main__":
    args = tumblrmapper.parse_args()
    tumblrmapper.configure_logging(args)

    blogs_toscrape = SCRIPTDIR + "tools/blogs_toscrape_test.txt"
    archives_toload = SCRIPTDIR +  "tools/1280_files_list.txt"
    database = Database(db_filepath="/home/firebird/tumblrmapper_test.fdb", \
                        username="sysdba", password="masterkey")
    test_jsons = ["vgf_latest.json", "3dandy.json", "thelewd3dblog_offset0.json",
     "leet_01.json", "cosm_01.json"]

    con = database.connect()
    # create_blank_database(database)
    # populate_db_with_blogs(database, blogs_toscrape)
    # Optional archives too
    # populate_db_with_archives(database, archives_toload)

    for jsonfile in test_jsons:
        print("Opening: {0}".format(jsonfile))
        myjson = json.load(open(SCRIPTDIR + "tools/test/" + jsonfile, 'r'))

        processed = unittest_update_table(database, con, myjson)
        print(BColors.BLUEOK + "processed: {0}".format(processed) + BColors.ENDC)
    con.close()

