from html.parser import HTMLParser
from urllib import parse

class urlFinder(HTMLParser):

    def __init__(self, pageURL):
        super().__init__()
        self.pageURL = pageURL
        self.setOfURLs = set()

    def handle_starttag(self, tag, attrs):
        if(tag == 'a'):
            for(attribute, val) in attrs:
                if(attribute == 'href'):
                    newURL = parse.urljoin(self.pageURL, val)
                    self.setOfURLs.add(newURL)


    def getSetOfURLs(self):
        return self.setOfURLs