TumblrMapper - maps blogs and posts from tumblr

dependencies: 

fake\_useragent
fdb (firebird python driver)


#PROBLEM# 

Tumblr serves compressed and downsized files by default (with suffix \_1280 for example).
Fortunately for us, they also keep \_raw original files on their CDN.
Unfortunately, they do no document this anywhere, it's essentially hidden from the public.
The original \_raw files are hidden behind paths such as:
http://data.tumblr.com/{SHA1}/tumblr\_{hash}\_raw.jpg
where {SHA1} is the sha1sum of the _original_ file (the \_raw one). 
This makes it impossible to retrieve files without knowing the sha1sum beforehand.

This tool scrapes all URLs on a supplied list of tumblr blogs and stores the following in a firebird database:
* Blog names, total posts, last updated
* Posts, remote\_id, post URL
* Posts' context (text content only) and most importantly
* File's URLs (both tumblr and all other detected valid URLs)

Optionally, if you have a list of \_1280 files, you can store them in the DB and look for the
corresponding \_raw files with provided tools.

#TODO#:

* Command line argument to force recheck DONE blogs with post\_scraped == total\_posts
