CREATE DATABASE blank_db.fdb

/* DOMAINS */

CREATE DOMAIN LONG_TEXT AS
VARCHAR(500);

CREATE DOMAIN SUPER_LONG_TEXT AS
VARCHAR(32765)

CREATE DOMAIN BOOLEAN AS smallint
CHECK (VALUE IS NULL OR VALUE IN (0, 1));

-- TABLE: BLOG
-- ID or BLOG

-- TABLE: POSTS
-- ID or POST | BLOG ID | CONTEXT | TITLE 
-- TABLE: URLS 
-- ID or URL | FILENAME | URL | POST ID 
-- or composite key of filename+url?

/* Note: Updating a table with indexes takes more time than updating a table without (because the indexes also need an update). 
So, only create indexes on columns that will be frequently searched against.*/

CREATE TABLE URLS (
    FILENAME varchar(255), -- derive from URL? omit field entirely?
    URL LONG_TEXT NOT NULL
    -- FOREIGN KEY REFERENCES POSTS(TID)
)
CREATE TABLE POSTS (
    TID INT NOT NULL PRIMARY KEY, --tumblr post id, unique
    post_url LONG_TEXT, --post_url of the reblog or post
    DATE varchar(255) -- tumblr supplied "date" field
)
CREATE TABLE CONTEXTS (
    ID INT PRIMARY KEY AUTOINCREMENT,
    TID INT NOT NULL,  --foreign key?
    CONTEXT LONG_TEXT UNIQUE
)

CREATE TABLE BLOGS (
    ID varchar(255) NOT NULL PRIMARY KEY,
    TOTAL_POSTS int DEFAULT(0),
    SCRAPED_POSTS int,
    STATUS varchar(50), --OK, DEAD, WIPED 
    LAST_SCRAPED SMALLDATETIME DEFAULT NOW()
)

-- INDICES: CREATE INDEX index_name ON table_name (column1, column2, ...);
CREATE INDEX IDX_URLS on URLS (URL) 

-- INSERTS: we got our response object with all fields
-- check blog? update time?
-- update post tid
-- update context
-- update urls

--initial submission:
INSERT INTO BLOGS (ID,TOTAL_POSTS, LAST_SCRAPED) VALUES (myblogname, mytotalpost)
INSERT INTO URL (FILENAME,URL) VALUES (myfilename,myurl);

--update blog entry
UPDATE URL SET URL VALUES (myurl1)
UPDATE BLOGS SET LAST_SCRAPED; 

--create a view to return context for a specific url?

-- create procedures to update tables faster https://www.youtube.com/watch?v=qykBERl-Xcg
CREATE PROCEDURE updateurl 
--create procedure to update datetime in BLOGS
CREATE PROCEDURE updateblogs