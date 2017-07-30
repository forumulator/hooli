from crawler import crawler

url = '<url_to_crawl>'

crawlerObj = crawler()

proxyDict = {'http' : 'http://<proxy_username>:<proxy_passwd>@202.141.80.22:3128',
            'https': 'https://<proxy_username>:<proxy_passwd>@202.141.80.22:3128'}

crawlerObj.assignUrl(url)
setOfUrls = crawlerObj.crawlUrl(proxyDict)

for url in setOfUrls:
    print(url)
