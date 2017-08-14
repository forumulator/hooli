from html.parser import HTMLParser
from urllib import parse

class linkFinder(HTMLParser):

    def error(self, message):
        pass

    def __init__(self, baseURL,pageURL):
        super().__init__()
        self.baseURL = baseURL
        self.pageURL = pageURL
        self.links = set()

# If tag is 'a', then looks for the 'href' attribute to find the urls in the page.
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for (attribute,value) in attrs:
                if attribute == 'href':
                    url = parse.urljoin(self.pageURL,value)
                    self.links.add(url)

        #if tag == 'frame':
        #    for (attribute,value) in attrs:
        #        if attribute == 'src':
        #            url = parse.urljoin(self.pageURL,value)
        #            self.links.add(url)

# Returns the set of links.
    def pageLinks(self):
        return self.links

