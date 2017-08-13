from spider_controller import spider_controller

url = 'https://en.wikipedia.org/wiki/Rick_and_Morty'

proxyDict = {'http' : 'http://<proxy_username>:<proxy_passwd>@202.141.80.22:3128',
            'https': 'https://<proxy_username>:<proxy_passwd>@202.141.80.22:3128'}

spider = spider_controller(5, 100, url)
spider.run()

print("Done!")
