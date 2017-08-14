import logger

from spider_controller import spider_controller

url = 'https://en.wikipedia.org/wiki/Rick_and_Morty'
pages_to_crawl = int(input("Enter pages to crawl: "))

proxyDict = {'http' : 'http://<proxy_username>:<proxy_passwd>@202.141.80.22:3128',
            'https': 'https://<proxy_username>:<proxy_passwd>@202.141.80.22:3128'}

logger.initialize()

print("Starting spider for %d pages" %pages_to_crawl)
spider = spider_controller(5, pages_to_crawl, url)
spider.run()

print("Done!")
