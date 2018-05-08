# TumblrMapper - maps blogs and posts from tumblr #

## PROBLEM ##

Tumblr serves compressed and downsized files by default (with suffix \_1280 for example).
Fortunately for us, they also keep \_raw original files on their CDN.
Unfortunately, they do no document this anywhere, it's essentially hidden from the public.
The original \_raw files are hidden behind paths such as:
`http://data.tumblr.com/{SHA1}/tumblr_{hash}_raw.jpg`
where {SHA1} is the sha1sum of the _original_ file (the \_raw one). 
This makes it impossible to retrieve files without knowing the sha1sum beforehand.

This tool scrapes all URLs on a supplied list of tumblr blogs and stores the following in a firebird database:
* Blog names, total posts, last updated
* Posts, remote\_id, post URL
* Posts' context (text content only) and most importantly
* File's URLs (both tumblr and all other detected valid URLs)

Optionally, if you have a list of \_1280 files, you can store them in the DB and look for the
corresponding \_raw files with provided tools.

## HOWTO USE ##

* Get some API keys (at least one) and copy them into api\_keys.json
* ./tumblrmapper -n to create a new database
* ./tumblrmapper -b to insert blogs recorded by the user 'blogs\_to\_scrape' text file (path in config), and update new ones
* ./tumblrmapper -s to populate the '1280' table with the list of files which the user is looking to recover their \_raw versions (their URLs)
* ./tumblrmapper -u to create the above file from VVV (Virtual Volume View) catalogs, eliminating \_raws already downloaded and recorded in a downloads.VVV catalog

## NOTES ##

* -i (--ignore\_duplicates) will keep rescraping posts which have already been added to DB
* -f (--dead\_blogs) will fetch all reblogs from blogs marked as DEAD and populate the DB with all blogs found in notes for each post
* -l DEBUG to force debug output in stdout
* Only tested in GNU/Linux

## Dependencies ##: 

[fake\_useragent](https://pypi.org/project/fake-useragent/)

[fdb (firebird python driver)](https://www.firebirdsql.org/en/devel-python-driver/)

[re2](https://github.com/andreasvc/pyre2)


## TODO ##:

* Command line argument to force recheck DONE blogs with post\_scraped == total\_posts
