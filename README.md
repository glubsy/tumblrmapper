##tumblrgator##

Files:
config: holds 
queue_toscrape: list of tumblr accounts to scrape
queue_scraping: list of accounts currently being sraped (for resuming)
queue_scraped: list of accounts fully scaped
scraper.log: log everything
blank.db: blank database for starting

Arguments:
- defaults:
    -c config: config is in the script path
    -t queue_toscape: script path
    -r queue_scraping: script path
    -d queue_scraped: script path
    -l scraper.log: script path
    -db blank.db: script path
    -v1: use API v1
    -v2: use API v2
    -v3: use infinite scroll scraping from archive (https://www.youtube.com/watch?v=EelmnSzykyI) STUB

if we use proxies, use threads + queue, otherwise maybe not?