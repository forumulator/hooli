import urllib.request
import urllib.error

import urlFinder


class crawler:

    def __init__(self):
        urlBeingCrawled = ''
        isIdle = True

    def assignUrl(self,urlToCrawl):
        self.isIdle = False
        self.urlBeingCrawled = urlToCrawl


    def crawlUrl(self, proxyDict = None):
        '''
        sends request for webpage, stores the response to file, and gathers links
        from the web page.  
        '''

        try:

            if proxyDict is not None:
                proxy_support = urllib.request.ProxyHandler(proxyDict)
                opener = urllib.request.build_opener(proxy_support)
                urllib.request.install_opener(opener)

            urlResponse = urllib.request.urlopen(self.urlBeingCrawled)
            htmlBytes = urlResponse.read()

            #assuming the web page is encoded in UTF-8
            htmlString = htmlBytes.decode('utf-8')

            self.storeWebPageToFile(htmlString, 'testFile.html', 'utf-8')

            return self.findOutgoingURLs(self.urlBeingCrawled, htmlString)

        except urllib.error.URLError:
            print('Error for url ' + self.urlBeingCrawled)
            print(error.URLError.reason)

    def storeWebPageToFile(self, htmlString, nameOfFile, encoding='utf-8'):
        f = open(nameOfFile, 'w', encoding = encoding)
        f.write(htmlString)
        f.close()

    def findOutgoingURLs(self, url, htmlString):
        urlFinderObj = urlFinder.urlFinder(url)
        urlFinderObj.feed(htmlString)
        return urlFinderObj.getSetOfURLs()