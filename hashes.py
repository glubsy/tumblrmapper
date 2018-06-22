#!/usr/bin/env python3.6
import logging
import os
import re
import sys
import json
import time
from collections import Counter
from collections import defaultdict
from threading import Lock, Event

import instances
import tumblrmapper
import db_handler
from constants import BColors

# capture the 6 letter hash after 1
re_6letters = re.compile(r'tumblr_.*?1(.{6})\w{0,2}(?:_.{2})?_\d{3,4}\..*', re.I)
# re_6letters_base = re.compile(r'tumblr_.*1(.{6})\w{0,2}', re.I)
re_6letters_base = re.compile(r'tumblr_.*?.{10}1(.{6}).{0,3}?', re.I)


# get each like tumblr_ url in table URLS
# for each, get post_id, r_id
# if post_id, associate hash with blogname
# if r_id, associate with reblogged_blogname

# we want a list / dict
# hashes -> blogname


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
    """Returns a list of the most common items in _set"""
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


def list_to_string(_list, default='None'):
    """format to pass in an sql query for a record's field,
    returns None if empty list"""
    if not _list:
        # return
        return default
    return ','.join(_list)


def update_hash_in_db(con, blog, normal_hashes, inline_hashes):
    """Updates blog row with each set of 3 pausible hashes"""
    cur = con.cursor()
    try:
        cur.execute(r"""update BLOGS set HASH = ?, INLINE_HASH = ? where blog_name = ?;""",
        (list_to_string(normal_hashes), list_to_string(inline_hashes, default=None), blog))
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


def compute_hashes(db):
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
    db.close_connection()

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


def lookup_blog_for_hash_in_blogs(con, _hash):
    """Returns a list of blogs which have this hash as a potential hash
    in their hash colum"""
    cur = con.cursor()
    try:
        cur.execute(r"""
select blog_name, hash, inline_hash from blogs
where hash similar to '%?,?""" + _hash + r""",?%?'
or inline_hash similar to '%?,?""" + _hash + r""",?%?';
""")
        #[(blogname, hash, inlinehash), (blogname, hash, inlinehash), (blogname, hash, inlinehash)]
        return cur.fetchall()
    except BaseException as e:
        logging.error(f"Exception when lookup hash {_hash}: {e}")
    finally:
        con.rollback()


def get_blog_name_from_reversed_lookup_in_urls(con, _hash, numrows=1):
    """Reversed lookup for hash in URLs and figure out the blog from reblogs remote_id
    if the last row is empty, there was an error from tumblr and we instead only have
    the reblogger's post. If remote_id is null, that's the original poster."""

    cur = con.cursor()
    try:
        cur.execute(r"""
select u.file_url, u.post_id, u.remote_id, b2.blog_name, b1.blog_name, p.post_url 
from (select * from urls where file_url similar to '%tumblr\_%1{0}%' escape '\' rows {1}) as u 
left join posts as p on u.remote_id = p.remote_id or u.post_id = p.post_id 
left join blogs as b1 on b1.auto_id = p.reblogged_blogname 
left join blogs as b2 on b2.auto_id = p.origin_blogname rows {2};"""
.format(_hash, numrows, numrows))
        return cur.fetchall()
    except BaseException as e:
        logging.error(f"Exception when reverse lookup {_hash}: {e}")
    finally:
        con.rollback()


def get_lost_filenames(con):
    cur = con.cursor()
    try:
        cur.execute(r"""select filebasename from old_1280;""")
        return cur.fetchall()
    except BaseException as e:
        logging.error(f"Error while fetching lost filenames: {e}")
    finally:
        con.rollback()


def fetch_corresponding_hashes(db, result_txt):
    """lost_files are bases only one per line, result will be written """

    con = db.connect()

    # get list of lost filenames from DB
    lost_f = get_lost_filenames(con)
    logging.warning(f"We have currently {len(lost_f)} unique filenames to look for.")

    hash_dict = {}

    with open(result_txt, 'w') as result_f:
        lost_count = 0
        for filename in lost_f:
            match = re_6letters_base.search(filename[0])
            if match: #there should always be a match here anyway
                lost_count += 1
                if hash_dict.get(match.group(1)) is None: # already seen this hash
                    hash_dict.setdefault(match.group(1), {'files': list(), 'blogname': set()})

                 # skip anymore blog lookups if we already have done one lookup for this hash
                if not len(hash_dict.get(match.group(1)).get('blogname')):
                    blog_list = lookup_blog_for_hash_in_blogs(con, match.group(1))

                    if not len(blog_list):
                        # we haven't computed which blog this hash belongs to yet
                        posters = set()
                        for row in get_blog_name_from_reversed_lookup_in_urls(con, match.group(1), numrows=20):
                            if row[-2] is None or row[-4] is None: # sadly, we have to use remote_id only
                                # the api screwed up and didn't give us the remote_id
                                # or it's an original post, but we can't be sure here
                                posters.add(row[-3])
                                continue
                            hash_dict[match.group(1)].get('blogname').add(row[-2])
                        # if we didn't get any from the reblogs, add the posters for manual filtering later
                        if not len(hash_dict[match.group(1)].get('blogname')):
                            for poster in posters:
                                hash_dict[match.group(1)].get('blogname').add(poster + '??')
                    else:
                        for item in blog_list:
                            hash_dict[match.group(1)].get('blogname').add(item[0])

                hash_dict[match.group(1)].get('files').append(filename[0].strip('\n'))


        # convert sets to list for json serializing, since sets don't work
        for key in hash_dict.keys():
            hash_dict[key]['blogname'] = list(hash_dict[key].get('blogname'))
            hash_dict[key]['files'].sort()
        logging.warning(f"Matched {lost_count} lines with regex")

        json.dump(hash_dict, result_f, indent=True)

        return hash_dict


def fetch_actual_origin(result_json, **kwargs):
    """For hashes where we are not sure of the actual source, go fetch a post to figure it out
    The most common on will be the first in the list and we'll replace said list if there's
    any item with '??'"""

    with open(result_json, 'r') as f:
        hash_dict = json.load(f)

    deep_scrape = dict(reblog_info=True)

    requester = tumblrmapper.Requester(pill2kill=kwargs.get('pill2kill'), lock=kwargs.get('lock'))
    update = tumblrmapper.UpdatePayload()
    count = 0
    con = kwargs.get('db').connect()

    for key in hash_dict.keys():
        cur_bloglist = list()
        for blog in hash_dict[key]['blogname']:
            if blog.find('??') != -1:
                print(f"Getting info on hash: {key}")
                for row in get_blog_name_from_reversed_lookup_in_urls(con, key, numrows=5):
                    update.__init__()
                    requester.name = row[-3]
                    tumblrmapper.post_get_wrapper(requester, update, deep_scrape, row[-5]) #post_id to fetch
                    try:
                        cur_bloglist.extend(parse_update(update))
                    except:
                        continue
                break # only do if one
        if cur_bloglist:
            print(f"Replacing blognames in {key}: {hash_dict[key]['blogname']}")
            hash_dict[key]['blogname'] = weigh_sets(cur_bloglist)
            print(f"Replaced blognames in {key} by {hash_dict[key]['blogname']}")
            count += 1
    print(f"Done updating {count} hashes")

    result_file_path = result_json.split('.json')[-2] + time.strftime('%Y%m%d_%H%M%S') + ".json"
    with open(result_file_path, 'w') as result_f:
        json.dump(hash_dict, result_f, indent=True)
    print(f"Finished writing result json to file {result_file_path}")


def parse_update(update):
    """Get origins of the reblogged post and returns them in a tuple"""

    if not update.posts_response:
        raise BaseException("Response list was empty!")
    blogslist = set()
    blogslist.add(update.posts_response[0].get('reblogged_from_name'))
    blogslist.add(update.posts_response[0].get('reblogged_root_name'))

    return list(blogslist)


def reset_blogs_by_hash(db, _json):
    """Read our blog hash pair json file, resets status in DB for blogs found"""
    con = db.connect()
    cur = con.cursor()

    with open(_json, 'r') as f:
        hash_dict = json.load(f)
    hash_count = 0
    blogname_count = 0
    for key in hash_dict.keys():
        hash_count += 1
        for blogname in hash_dict.get(key).get('blogname'):
            if blogname.find('??') != 1:
                blogname = blogname.strip('??')
            blogname_count += 1
            try:
                # insert or update with priority 200 and status "new"
                cur.callproc(r"insert_blogname", (blogname, 'new', 200))
            except BaseException as e:
                logging.error(f"Error while updating: {e}")
            finally:
                con.commit()
            logging.warning(f"Reset status for {blogname} to 'new'.")
    logging.warning(f"{hash_count} hashes found. {blogname_count} blogs reset.")


def main(args, kwargs):
    SCRIPTDIR = os.path.dirname(__file__)
    result_hash_pairs = SCRIPTDIR + os.sep + "tools/lost_file_blog_hash_pairs.json"

    if args.compute_hashes:
        compute_hashes(kwargs.get('db'))
    elif args.match_hashes:
        if os.path.exists(result_hash_pairs):
            fetch_actual_origin(result_hash_pairs, **kwargs)
        else:
            fetch_corresponding_hashes(kwargs.get('db'), result_hash_pairs)
    elif args.reset_blogs_by_hash:
        reset_blogs_by_hash(kwargs.get('db'), result_hash_pairs)


if __name__ == "__main__":
    import tumblrmapper
    args = tumblrmapper.parse_args()
    tumblrmapper.setup_config(args)

    db = db_handler.Database(db_filepath=instances.config.get('tumblrmapper', 'db_filepath')
                        + os.sep + instances.config.get('tumblrmapper', 'db_filename'),
                        username=instances.config.get('tumblrmapper', 'username'),
                        password=instances.config.get('tumblrmapper', 'password'))
    pill2kill = Event()
    kwargs = dict(pill2kill=pill2kill, lock=Lock(), db=db)

    tumblrmapper.init_global_api_keys()
    tumblrmapper.init_global_proxies(THREADS=1, pill2kill=pill2kill)

    main(args, kwargs)


