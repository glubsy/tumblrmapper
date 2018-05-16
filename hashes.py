#!/usr/bin/env python3.6
import logging
import os
import re
import sys
from collections import Counter
import instances
from constants import BColors
from db_handler import Database

re_6letters = re.compile(r'tumblr_.*?1(.{6})\w{0,2}(?:_.{2})?_\d{3,4}\..*', re.I)

# get each like tumblr_ url
# for each, get post_id, r_id
# if post_id, associate hash with blogname
# if r_id, associate with reblogged_blogname

# we want a list / dict
# hashes -> blogname

# get each blog name's auto_id from bLOGS table
# -> get POSTS post_id, get file_url where like "tumblr\_"
# make column in BLOGS for the hash

def get_blog(con):
    """builds a list of hashes associated with blog name"""
    cur = con.cursor()
    query = cur.prep(r"""
EXECUTE BLOCK
returns (v_b d_blog_name)
as
BEGIN
    for select blog_name from blogs
    where (crawl_status is not null
    and CRAWLING != 1
    and hash is null)
    rows 1
    into :v_b
    as cursor tcur do
        update BLOGS set CRAWLING = 1 where current of tcur;
        suspend;
END
""")
    # cur.execute(r"""select * from blogs where blog_name = 'atrociousalpaca'""")
    
    try:
        cur.execute(query)
        return cur.fetchall()[0]
    except:
        con.rollback()
        raise
    finally:
        con.commit()


def get_combo_posts_and_urls(blog, con):
    cur = con.cursor()
    logging.warning(f"Getting posts for {blog}")
    cur.execute(r"""
select * from
(select p.o_post_id, p.o_remote_id, u.FILE_URL, p.o_origin_name, p.o_reblogged_name2
from (select * from FETCH_DEAD_POSTS('""" + blog + r"""')) as p
join urls u on p.o_post_id = u.POST_ID
where u.file_url SIMILAR TO '%tumblr\_%' escape '\'
UNION
select p.o_post_id, p.o_remote_id, u.FILE_URL, p.o_origin_name, p.o_reblogged_name2
from (select * from FETCH_DEAD_POSTS('""" + blog + r"""')) as p
join urls u on p.o_remote_id = u.REMOTE_ID
where u.file_url SIMILAR TO '%tumblr\_%' escape '\' ) as f order by f.file_url;""")
    # MEMO: this returns
    # o_post_id, o_remote_id, file_url, o_origin_name, o_reblogged_name2
    # o_origin_name is always the remote_id's owner, if there's one, otherwise null (original post)
    # o_reblogged_name2 is the reblogger, OR the actual poster of an original post (in which case, o_origin_name is null)
    try:
        return cur.fetchall()
    except:
        raise
    finally:
        con.rollback()


def weigh_sets(_set):
    """Returns a list of the 3 most common hashes in _set"""
    if _set == []:
        return _set
    c = Counter(_set)
    m = c.most_common()
    r = list()
    count = -1
    while count < len(m) - 1:
        count += 1
        try:
            r.append(m[count][0])
        except:
            break
    return r


def list_to_string(_list):
    """format to pass in an sql query for a record's field, 
    returns None if empty list"""
    if not _list:
        return None
    return ','.join(_list)


def update_hash_in_db(con, blog, normal_hashes, inline_hashes):
    """Updates blog row with each set of 3 pausible hashes"""
    cur = con.cursor()
    try:
        cur.execute(r"""update BLOGS set HASH = ?, INLINE_HASH = ? where blog_name = ?;""", 
        (list_to_string(normal_hashes), list_to_string(inline_hashes), blog))
    except BaseException as e:
        logging.error(f"{BColors.FAIL}Exception during update DB of hashes: {e}{BColors.ENDC}")
        raise
    finally:
        con.commit()
    logging.debug(f"{BColors.GREEN}Updated {blog} with potential hashes \
{normal_hashes}, {inline_hashes}{BColors.ENDC}")


def reset_crawling(con):
    cur = con.cursor()
    logging.warning("Resetting crawling field in DB...")
    try:
        cur.execute('''update BLOGS set CRAWLING = 0 where CRAWLING is not null;''')
    except:
        con.rollback()
        raise
    con.commit()


def main(db):
    con = db.connect()

    reset_crawling(con)

    while True:
        blog = get_blog(con)[0]
        if not blog:
            break
        normal_set = list()
        inline_set = list()
        logging.warning(f"{BColors.BLUE}Processing blog: {blog}{BColors.ENDC}")
        # cur = con.cursor()
        # cur.execute(r"""select * from FETCH_DEAD_POSTS('""" + blog[1] + r"""');""")
        count = 0
        for row in get_combo_posts_and_urls(blog, con):
            # logging.debug(f"{BColors.GREEN}got url {row}{BColors.ENDC}")
            count += 1

            if row[-1] == blog and row[-2] is not None and row[-2] != blog:
                # logging.debug(f"row[-1] == blog and row[-2] is not None and row[-2] != blog: {row}")
                continue  #we ignore, it's the blog reblogging someone else
            elif row[-2] is None and row[-1] == blog: #original post, not reblog -> priority?
                # logging.debug(f"row[-2] is None and row[-1] == blog: {row}")
                match = re_6letters.search(row[2])
                if match:
                    if match.group(0).find('inline') != -1:
                        inline_set.append(match.group(1))
                    else:
                        normal_set.append(match.group(1))
            elif row[-2] == blog:
                # logging.debug(f"row[-2] == blog: {row}")
                match = re_6letters.search(row[2])
                if match:
                    if match.group(0).find('inline') != -1:
                        inline_set.append(match.group(1))
                    else:
                        normal_set.append(match.group(1))

        logging.warning(f'Got {count} urls for {blog}')
        logging.debug(f'\nnormal_set length {len(normal_set)}: \
{normal_set[:10]} inline_set length {len(inline_set)}: {inline_set[:10]}')
        normal_hashes = weigh_sets(normal_set)[:3]
        inline_hashes = weigh_sets(inline_set)[:6]

        logging.warning(f"{BColors.MAGENTA}{blog} normal_hashes: {normal_hashes}, inline_hashes: {inline_hashes}{BColors.ENDC}")

        update_hash_in_db(con, blog, normal_hashes, inline_hashes)

    logging.warning(f'{BColors.GREEN}Done generating hashes for blogs.{BColors.ENDC}')

def get_posts_for_blog(blog, con):
    """sets of urls. DEPRECATED"""
    cur = con.cursor()
    logging.debug(f"get_posts_for_blog {blog}")
    cur.execute(r"""select * from FETCH_DEAD_POSTS('?');""", (blog,))
    try:
        return cur.fetchall()
    except:
        raise
    finally:
        con.rollback()


def get_url_for_id(post_id, con):
    """Fecthing urls for post. DEPRECATED too slow"""
    cur = con.cursor()
    cur.execute(
r"""select file_url from urls where (post_id = '?' and (file_url SIMILAR TO '%tumblr\_%' escape '\'))
union
select file_url from urls where (remote_id = '?' and (file_url SIMILAR TO '%tumblr\_%' escape '\'));
""", (post_id, post_id))
    try:
        return cur.fetchall()
    except:
        raise
    finally:
        con.rollback()


def ResultIter(cur):
    """An iterator that uses fetchmany to keep memory usage down. DEPRECATED"""
    while True:
        results = cur.fetchmany()
        if not results:
            break
        for result in results:
            yield result


if __name__ == "__main__":
    import tumblrmapper
    SCRIPTDIR = os.path.dirname(__file__) + os.sep
    args = tumblrmapper.parse_args()
    tumblrmapper.setup_config(args)

    # archives_toload = SCRIPTDIR +  "tools/1280_files_list.txt"
    database = Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                            + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                            username=instances.config.get('tumblrmapper', 'username'),
                            password=instances.config.get('tumblrmapper', 'password'))
    main(database)
