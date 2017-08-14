from html.parser import HTMLParser
from urllib import parse

class UrlFinder(HTMLParser):

    def __init__(self, pageURL):
        super().__init__()

        self.__pageURL = pageURL
        self.__setOfURLs = set()
        self.__pageURL_domain = (parse.urlparse(self.__pageURL)
                                 .netloc
                                 .split(":")[0])

    def handle_starttag(self, tag, attrs):
        if(tag == 'a'):
            for(attribute, val) in attrs:
                if(attribute == 'href'):
                    newURL = parse.urljoin(self.__pageURL, val)
                    newURL_domain = parse.urlparse(newURL).netloc.split(":")[0]

                    # check to ensure that pages of same domain are added
                    if newURL_domain == self.__pageURL_domain:
                        # check to remove links to different points of same
                        # page
                        self.__setOfURLs.add(newURL.split("#")[0])

    def getSetOfURLs(self):
        return self.__setOfURLs
