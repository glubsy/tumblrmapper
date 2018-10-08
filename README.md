# TumblrMapper - maps blogs and posts from tumblr #

## Rationale:

Tumblr serves compressed and downsized files by default (with suffix \_1280 for example).

~~Fortunately for us, they also keep \_raw original files on their CDN.~~

**__Update 2018-08-14__: tumblr now denies access to _raw files stored on their Amazon S3 buckets. This tools is currently useless.**

Unfortunately, they do not document this anywhere, it's essentially hidden from the public eye and knowledge.

The original \_raw files are hidden behind paths such as:
`http://data.tumblr.com/{SHA1}/tumblr_{hash}1{hash}_raw.jpg`
where {SHA1} is the sha1sum of the _original_ file (the \_raw one). 

This makes it impossible to retrieve files without knowing the sha1 checksum beforehand (TODO: write a script to brute-force SHA1 for each files to recover...?)

This tool scrapes all URLs on a supplied list of tumblr blogs and stores the following in a firebird database, in the hopes to find URLs for each \_1280 file already downloaded:
* Blog names, total posts, last updated
* Posts, post URL, reblogged\_id (remote\_id)
* Posts' context (text content only) optionally and most importantly
* File's URLs (all valid URLS found in each post)

Optionally, if we have a premade list of \_1280 files, we can store them in the DB and look for the
corresponding \_raw files with provided tools.

## How to use:

* Get some API keys (at least one) and copy them into api\_keys.json
* ./tumblrmapper -n to create a new database
* ./tumblrmapper -b to insert blogs recorded by the user 'blogs\_to\_scrape' text file (path in config), and update new ones
* ./tumblrmapper -s to populate the '1280' table with the list of files which the user is looking to recover their \_raw versions (their URLs)
* ./tumblrmapper -u to create the above file from VVV (Virtual Volume View) catalogs, eliminating \_raws already downloaded and recorded in a downloads.VVV catalog

## Notes: 

* -f (--ignore\_duplicates) will keep rescraping posts which have already been added to DB
* -i (--dead\_blogs) will fetch all reblogs from blogs marked as DEAD and populate the DB with all blogs found in notes for each post
* -l DEBUG to force debug output in stdout
* Only tested in GNU/Linux

## Dependencies: 

Python >= 3.6

[Firebird server 2.5](https://firebirdsql.org/)

Requests 2.18.4

lxml

[fake\_useragent](https://pypi.org/project/fake-useragent/)

[fdb (firebird python driver)](https://www.firebirdsql.org/en/devel-python-driver/)

[re2](https://github.com/andreasvc/pyre2) otherwise falls back to re, but can lead to a runaway process

~~[requests-oauthlib](https://pypi.org/project/requests-oauthlib/) to get followers/likes for each blog (doesn't work)~~

## TODO:

* Command line argument to force recheck DONE blogs with post\_scraped == total\_posts

## License:

This is just a personal project, the code is horrendous and messy. Therefore, the license is MIT.
