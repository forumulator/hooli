from HTMLParser import HTMLParser
from bs4 import BeautifulSoup

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

# removes the tags and only the content is returned
def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


# removing anything related to script and style
def removeTags(html, *tags):
    soup = BeautifulSoup(html)
    for tag in tags:
        for tag in soup.findAll(tag):
            tag.replaceWith(" ")

    return soup

# this is to prevent words from glueing together after tags are removed
def correction(html):
    ht=html.replace('</a>',' </a>')
    ht=ht.replace('<br>'," ")
    return ht


filelist=['doc1.html','doc2.html','doc3.html']    

for filename in filelist:
    f=open(filename,"r")
    y=unicode(f.read())
    fname=filename.replace('.html','.txt')
    ft=open(fname,"w")

    ft.write(y)

