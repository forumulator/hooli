from spider import spider
from domain import *
from general import *
import time, threading

#Define the Project name and base URL
PROJECT_NAME = 'Intranet'
HOMEPAGE = 'http://intranet.iitg.ernet.in/'
HOMEPAGE = urlFix(HOMEPAGE)
DOMAIN_NAME = getDomainName(HOMEPAGE)

#Initialize the spider
spider(PROJECT_NAME,HOMEPAGE,DOMAIN_NAME)

#check if there are links in queue, if so then crawl
def crawl():
    while len(spider.queue)>0:
        print(str(len(spider.queue))+ ' links in queue')
        print(str(len(spider.crawled))+ ' links crawled')
        link = spider.queue.pop()
        spider.crawlPage(link)

#crawl()

# Periodic run of the Crawler. Set time is 1 week.
def foo():
    crawl()
    print('Crawl Complete')
    threading.Timer(604800,foo).start()

foo()