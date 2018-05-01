TumblrMapper - maps blogs and posts from tumblr

dependencies: 

fake_useragent
fdb (firebird python driver)


#PROBLEM# 

Tumblr keeps serves compressed and downsized files by default. 
Fortunately for us, they also keep _raw original files on their CDN. 
Unfortunately, they do no document this anywhere, it's essentially hidden from the public.
This tool scrapes all URLs on a supplied list of tumblr blogs and stores them in a firebird database.

Optionally, if you have a list of _1280 files, you can store them in the DB and look for the
corresponding _raw files automatically.


#TODO#:

* Command line argument to force recheck DONE blogs with post_scraped == total_posts