# TumblrMapper - maps blogs and posts from tumblr #

## Rationale:

Tumblr serves compressed and downsized files by default (with suffix \_1280 for example).

Fortunately for us, they also keep \_raw original files on their CDN.

Unfortunately, they do no document this anywhere, it's essentially hidden from the public.

The original \_raw files are hidden behind paths such as:
`http://data.tumblr.com/{SHA1}/tumblr_{hash}_raw.jpg`
where {SHA1} is the sha1sum of the _original_ file (the \_raw one). 

This makes it impossible to retrieve files without knowing the sha1 checksum beforehand (TODO: write a script to brute-force SHA1 for each 4000 files to recover...)

This tool scrapes all URLs on a supplied list of tumblr blogs and stores the following in a firebird database, in the hopes to find URLs for each \_1280 file already downloaded:
* Blog names, total posts, last updated
* Posts, remote\_id, post URL
* Posts' context (text content only) and most importantly
* File's URLs (both tumblr's and all other detected valid URLs inside each post)

Optionally, if you have a list of \_1280 files, you can store them in the DB and look for the
corresponding \_raw files with provided tools.

## How to use:

* Get some API keys (at least one) and copy them into api\_keys.json
* ./tumblrmapper -n to create a new database
* ./tumblrmapper -b to insert blogs recorded by the user 'blogs\_to\_scrape' text file (path in config), and update new ones
* ./tumblrmapper -s to populate the '1280' table with the list of files which the user is looking to recover their \_raw versions (their URLs)
* ./tumblrmapper -u to create the above file from VVV (Virtual Volume View) catalogs, eliminating \_raws already downloaded and recorded in a downloads.VVV catalog

## Notes: 

* -i (--ignore\_duplicates) will keep rescraping posts which have already been added to DB
* -f (--dead\_blogs) will fetch all reblogs from blogs marked as DEAD and populate the DB with all blogs found in notes for each post
* -l DEBUG to force debug output in stdout
* Only tested in GNU/Linux

## Dependencies: 

[Firebird server 2.5](https://firebirdsql.org/)

[fake\_useragent](https://pypi.org/project/fake-useragent/)

[fdb (firebird python driver)](https://www.firebirdsql.org/en/devel-python-driver/)

[re2](https://github.com/andreasvc/pyre2) otherwise falls back to re, but can lead to a runaway process


## TODO:

* Command line argument to force recheck DONE blogs with post\_scraped == total\_posts
* Make firebird server optional, replace with [fdb\_embedded](https://github.com/andrewleech/fdb_embedded)if at all possible?

## License:

This is just a personal project, the code is horrendous and needs improvements. Therefore, the license is MIT.
