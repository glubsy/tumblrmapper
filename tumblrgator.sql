/********************* ROLES **********************/

/********************* UDFS ***********************/

/****************** SEQUENCES ********************/

CREATE SEQUENCE TBLOGS_AUTOID_SEQUENCE;
CREATE SEQUENCE TBLOGS_AUTOID_SEQUENCE2;
/******************** DOMAINS *********************/

CREATE DOMAIN D_AUTO_ID
 AS BIGINT
;
CREATE DOMAIN D_BLOG_NAME
 AS VARCHAR(60)
 COLLATE NONE;
CREATE DOMAIN D_BOOLEAN
 AS SMALLINT
 DEFAULT 0
 CHECK (VALUE IS NULL OR VALUE IN (0, 1, 2))
;
CREATE DOMAIN D_EPOCH
 AS BIGINT
;
CREATE DOMAIN D_HASH
 AS VARCHAR(25)
 COLLATE NONE;
CREATE DOMAIN D_INLINE_HASH
 AS VARCHAR(45)
 COLLATE NONE;
CREATE DOMAIN D_LONG_TEXT
 AS VARCHAR(500)
 COLLATE NONE;
CREATE DOMAIN D_POSTURL
 AS VARCHAR(300)
 COLLATE NONE;
CREATE DOMAIN D_POST_NO
 AS BIGINT
;
CREATE DOMAIN D_SUPER_LONG_TEXT
 AS VARCHAR(32765)
 COLLATE NONE;
CREATE DOMAIN D_URL
 AS VARCHAR(1000)
 COLLATE NONE;
/******************* PROCEDURES ******************/

SET TERM ^ ;
CREATE PROCEDURE FETCH_ALL_BLOG_S_POSTS (
    I_PARAM VARCHAR(10) )
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
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE FETCH_DEAD_POSTS (
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
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE FETCH_ONE_BLOGNAME (
    I_STATUS_TYPE VARCHAR(10) DEFAULT 'resume' )
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
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE FETCH_ONE_DONE_BLOGNAME
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
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE INSERT_ARCHIVE (
    I_F VARCHAR(60),
    I_FB VARCHAR(60),
    I_P VARCHAR(100) )
RETURNS (
    F D_BOOLEAN )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE INSERT_BLOGNAME (
    I_BLOGNAME D_BLOG_NAME,
    I_CRAWL_STATUS VARCHAR(10) DEFAULT 'new',
    I_PRIO SMALLINT DEFAULT null )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE INSERT_BLOGNAME_GATHERED (
    I_BLOGNAME D_BLOG_NAME,
    I_NEW VARCHAR(10) DEFAULT NULL )
RETURNS (
    O_GENERATED_AUTO_ID D_AUTO_ID,
    O_CRAWL_STATUS VARCHAR(10) )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE INSERT_POST (
    I_POSTID D_POST_NO,
    I_BLOG_ORIGIN D_BLOG_NAME,
    I_POST_URL D_POSTURL,
    I_POST_DATE D_EPOCH,
    I_REMOTEID D_POST_NO DEFAULT null,
    I_REBLOGGED_BLOG_NAME D_BLOG_NAME DEFAULT null,
    I_NOTES INTEGER DEFAULT null )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE INSERT_URL (
    I_URL D_URL,
    I_POST_ID D_POST_NO,
    I_REMOTE_ID D_POST_NO DEFAULT null )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE RESET_BLOG_CRAWLING_STATUS (
    I_NAME D_BLOG_NAME )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE RESET_CRAWL_STATUS (
    I_BLOG_NAME D_BLOG_NAME,
    I_RESET_TYPE VARCHAR(10) )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE RESET_CRAWL_STATUS_ALL
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE UPDATE_BLOG_INFO (
    I_NAME D_BLOG_NAME,
    I_HEALTH VARCHAR(5),
    I_TOTAL INTEGER,
    I_UPDATED D_EPOCH,
    I_OFFSET INTEGER,
    I_SCRAPED INTEGER,
    I_STATUS VARCHAR(10) DEFAULT 'resume',
    I_CRAWLING D_BOOLEAN DEFAULT 0 )
RETURNS (
    O_HEALTH VARCHAR(5),
    O_TOTAL_POSTS INTEGER,
    O_UPDATED D_EPOCH,
    O_LAST_CHECKED D_EPOCH,
    O_OFFSET INTEGER,
    O_SCRAPED INTEGER )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

SET TERM ^ ;
CREATE PROCEDURE UPDATE_BLOG_INFO_INIT (
    I_NAME D_BLOG_NAME,
    I_HEALTH VARCHAR(5),
    I_TOTAL INTEGER,
    I_UPDATED D_EPOCH,
    I_STATUS VARCHAR(10) DEFAULT 'resume',
    I_CRAWLING D_BOOLEAN DEFAULT 0 )
RETURNS (
    O_HEALTH VARCHAR(5),
    O_TOTAL_POSTS INTEGER,
    O_UPDATED D_EPOCH,
    O_LAST_CHECKED D_EPOCH,
    O_OFFSET INTEGER,
    O_SCRAPED INTEGER )
AS
BEGIN SUSPEND; END^
SET TERM ; ^

/******************** TABLES **********************/

CREATE TABLE BLOGS
(
  AUTO_ID D_AUTO_ID NOT NULL,
  BLOG_NAME D_BLOG_NAME,
  HEALTH VARCHAR(5),
  CRAWL_STATUS VARCHAR(10) DEFAULT NULL,
  CRAWLING D_BOOLEAN DEFAULT 0,
  TOTAL_POSTS INTEGER,
  POST_OFFSET INTEGER,
  POSTS_SCRAPED INTEGER,
  LAST_CHECKED D_EPOCH,
  LAST_UPDATE D_EPOCH,
  PRIORITY SMALLINT,
  HASH D_HASH,
  INLINE_HASH D_INLINE_HASH,
  CONSTRAINT INTEG_2 PRIMARY KEY (AUTO_ID),
  CONSTRAINT BLOGNAMES_UNIQUE UNIQUE (BLOG_NAME)
  USING INDEX IX_BLOGNAMES
);
CREATE TABLE CRAWLING
(
  BLOG_NAME D_BLOG_NAME NOT NULL,
  CRAWL_STATUS VARCHAR(20),
  CONSTRAINT INTEG_21 PRIMARY KEY (BLOG_NAME)
);
CREATE TABLE OLD_1280
(
  FILENAME VARCHAR(60) NOT NULL,
  FILEBASENAME VARCHAR(60),
  PATH VARCHAR(10000),
  CONSTRAINT INTEG_17 PRIMARY KEY (FILENAME)
);
CREATE TABLE POSTS
(
  POST_ID D_POST_NO NOT NULL,
  REMOTE_ID D_POST_NO,
  ORIGIN_BLOGNAME D_AUTO_ID NOT NULL,
  REBLOGGED_BLOGNAME D_AUTO_ID,
  POST_URL D_POSTURL NOT NULL,
  POST_DATE D_EPOCH,
  NOTES INTEGER,
  CONSTRAINT INTEG_4 PRIMARY KEY (POST_ID)
);
CREATE TABLE URLS
(
  FILE_URL D_URL NOT NULL,
  POST_ID D_POST_NO NOT NULL,
  REMOTE_ID D_POST_NO,
  CONSTRAINT INTEG_13 PRIMARY KEY (FILE_URL)
);
/********************* VIEWS **********************/

CREATE VIEW POSTS_WITH_NAMES (POST_ID, ORIGIN_BLOGNAME, REBLOGGED_BLOGNAME, 
    AUTO_ID, BLOG_NAME)
AS 
select p.post_id, p.ORIGIN_BLOGNAME, p.REBLOGGED_BLOGNAME, b.auto_id, b.BLOG_NAME
from posts as p
inner join blogs as b on b.auto_id = p.REBLOGGED_BLOGNAME;
CREATE VIEW POSTS_WITH_REBLOGGED_NAME (POST_ID, ORIGIN_BLOGNAME, 
    REBLOGGED_BLOGNAME, AUTO_ID, BLOG_NAME)
AS 
select p.post_id, p.ORIGIN_BLOGNAME, p.REBLOGGED_BLOGNAME, b.auto_id, b.BLOG_NAME
from posts as p
inner join blogs as b on b.auto_id = p.REBLOGGED_BLOGNAME;
/******************* EXCEPTIONS *******************/

/******************** TRIGGERS ********************/


SET TERM ^ ;
ALTER PROCEDURE FETCH_ALL_BLOG_S_POSTS (
    I_PARAM VARCHAR(10) )
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
end^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE FETCH_DEAD_POSTS (
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
end^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE FETCH_ONE_BLOGNAME (
    I_STATUS_TYPE VARCHAR(10) DEFAULT 'resume' )
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
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE FETCH_ONE_DONE_BLOGNAME
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
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE INSERT_ARCHIVE (
    I_F VARCHAR(60),
    I_FB VARCHAR(60),
    I_P VARCHAR(100) )
RETURNS (
    F D_BOOLEAN )
AS
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
end^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE INSERT_BLOGNAME (
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
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE INSERT_BLOGNAME_GATHERED (
    I_BLOGNAME D_BLOG_NAME,
    I_NEW VARCHAR(10) DEFAULT NULL )
RETURNS (
    O_GENERATED_AUTO_ID D_AUTO_ID,
    O_CRAWL_STATUS VARCHAR(10) )
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
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE INSERT_POST (
    I_POSTID D_POST_NO,
    I_BLOG_ORIGIN D_BLOG_NAME,
    I_POST_URL D_POSTURL,
    I_POST_DATE D_EPOCH,
    I_REMOTEID D_POST_NO DEFAULT null,
    I_REBLOGGED_BLOG_NAME D_BLOG_NAME DEFAULT null,
    I_NOTES INTEGER DEFAULT null )
AS
declare variable v_blog_origin_id d_auto_id;
declare variable v_fetched_reblogged_blog_id d_auto_id default null;
declare variable v_b_update_gathered d_boolean default 0;
BEGIN

select AUTO_ID from BLOGS where BLOG_NAME = :i_blog_origin into :v_blog_origin_id;
if (:v_blog_origin_id is null) THEN 
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
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE INSERT_URL (
    I_URL D_URL,
    I_POST_ID D_POST_NO,
    I_REMOTE_ID D_POST_NO DEFAULT null )
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
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE RESET_BLOG_CRAWLING_STATUS (
    I_NAME D_BLOG_NAME )
AS
BEGIN
delete from CRAWLING where BLOG_NAME = :i_name;
update BLOGS set CRAWLING = 0 where BLOG_NAME = :i_name;
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE RESET_CRAWL_STATUS (
    I_BLOG_NAME D_BLOG_NAME,
    I_RESET_TYPE VARCHAR(10) )
AS
declare variable v_crawl varchar(10) default null;
BEGIN
select (CRAWL_STATUS) from BLOGS where (BLOG_NAME = :i_blog_name) into :v_crawl;
if (v_crawl is not null) THEN
update BLOGS set CRAWL_STATUS = :i_reset_type where (BLOG_NAME = :i_blog_name);
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE RESET_CRAWL_STATUS_ALL
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

END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE UPDATE_BLOG_INFO (
    I_NAME D_BLOG_NAME,
    I_HEALTH VARCHAR(5),
    I_TOTAL INTEGER,
    I_UPDATED D_EPOCH,
    I_OFFSET INTEGER,
    I_SCRAPED INTEGER,
    I_STATUS VARCHAR(10) DEFAULT 'resume',
    I_CRAWLING D_BOOLEAN DEFAULT 0 )
RETURNS (
    O_HEALTH VARCHAR(5),
    O_TOTAL_POSTS INTEGER,
    O_UPDATED D_EPOCH,
    O_LAST_CHECKED D_EPOCH,
    O_OFFSET INTEGER,
    O_SCRAPED INTEGER )
AS
declare variable v_checked d_epoch;
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
    else if (:i_crawling = 0) THEN
        delete from CRAWLING where BLOG_NAME = :i_name;
END^
SET TERM ; ^


SET TERM ^ ;
ALTER PROCEDURE UPDATE_BLOG_INFO_INIT (
    I_NAME D_BLOG_NAME,
    I_HEALTH VARCHAR(5),
    I_TOTAL INTEGER,
    I_UPDATED D_EPOCH,
    I_STATUS VARCHAR(10) DEFAULT 'resume',
    I_CRAWLING D_BOOLEAN DEFAULT 0 )
RETURNS (
    O_HEALTH VARCHAR(5),
    O_TOTAL_POSTS INTEGER,
    O_UPDATED D_EPOCH,
    O_LAST_CHECKED D_EPOCH,
    O_OFFSET INTEGER,
    O_SCRAPED INTEGER )
AS
declare variable v_checked d_epoch;
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
update or insert into CRAWLING (BLOG_NAME, CRAWL_STATUS)
    values (:i_name, 'progress') MATCHING (BLOG_NAME);
END^
SET TERM ; ^


ALTER TABLE POSTS ADD CONSTRAINT INTEG_7
  FOREIGN KEY (ORIGIN_BLOGNAME) REFERENCES BLOGS (AUTO_ID);
ALTER TABLE POSTS ADD CONSTRAINT INTEG_8
  FOREIGN KEY (REBLOGGED_BLOGNAME) REFERENCES BLOGS (AUTO_ID);
CREATE INDEX IX_REMOTE_IDS ON POSTS (REMOTE_ID);
ALTER TABLE URLS ADD CONSTRAINT INTEG_15
  FOREIGN KEY (POST_ID) REFERENCES POSTS (POST_ID);
GRANT EXECUTE
 ON PROCEDURE FETCH_ALL_BLOG_S_POSTS TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE FETCH_DEAD_POSTS TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE FETCH_ONE_BLOGNAME TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE FETCH_ONE_DONE_BLOGNAME TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE INSERT_ARCHIVE TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE INSERT_BLOGNAME TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE INSERT_BLOGNAME_GATHERED TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE INSERT_POST TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE INSERT_URL TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE RESET_BLOG_CRAWLING_STATUS TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE RESET_CRAWL_STATUS TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE RESET_CRAWL_STATUS_ALL TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE UPDATE_BLOG_INFO TO  SYSDBA;

GRANT EXECUTE
 ON PROCEDURE UPDATE_BLOG_INFO_INIT TO  SYSDBA;

GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE
 ON BLOGS TO  SYSDBA WITH GRANT OPTION;

GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE
 ON CRAWLING TO  SYSDBA WITH GRANT OPTION;

GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE
 ON OLD_1280 TO  SYSDBA WITH GRANT OPTION;

GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE
 ON POSTS TO  SYSDBA WITH GRANT OPTION;

GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE
 ON URLS TO  SYSDBA WITH GRANT OPTION;

GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE
 ON POSTS_WITH_NAMES TO  SYSDBA WITH GRANT OPTION;

GRANT DELETE, INSERT, REFERENCES, SELECT, UPDATE
 ON POSTS_WITH_REBLOGGED_NAME TO  SYSDBA WITH GRANT OPTION;

