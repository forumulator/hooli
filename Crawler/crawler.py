import logging

from urllib import error
from urllib import request

from urlFinder import UrlFinder

FILE_STORAGE_ENCODING = "utf-8"
FILE_STORAGE_BASE_PATH = "html/"

SetOfURLs_KEY = 'setOfURLs'
HTMLString_KEY = 'htmlString'

class Crawler:

    def __init__(self):
        self.__urlBeingCrawled = None

    def assignUrl(self, urlToCrawl):
        # checks for URL can be done here
        self.__urlBeingCrawled = urlToCrawl

    def crawlUrl(self, proxyDict=None, storeToFileFlag=False):
        '''
        sends request for webpage, stores the response to file, and gathers
        links from the web page.
        '''

        if self.__urlBeingCrawled is None:
            raise ValueError("URL has not been assigned to crawler!")

        htmlString = None
        outgoingURLSet = None

        urlResponse = self.__openURL(proxyDict)

        if urlResponse is not None:
            # import hashlib
            
            responseCharset = urlResponse.headers.get_content_charset()
            if(responseCharset is not None):
                htmlString = self.__decodeResponse(urlResponse, responseCharset)
            else:
                htmlString = self.__decodeResponse(urlResponse)

            # url_hash = hashlib.sha256(bytes(self.__urlBeingCrawled, "utf-8")).hexdigest()
            # filename = (FILE_STORAGE_BASE_PATH
            #             + url_hash
            #             + ".html")
            # self.__storeWebPageToFile(htmlString,
            #                           filename,
            #                           FILE_STORAGE_ENCODING)

            outgoingURLSet = self.__findOutgoingURLs(self.__urlBeingCrawled,
                                                     htmlString)

        return htmlString, outgoingURLSet

    def __openURL(self, proxyDict):
        '''
        Returns Response object obtained from request.urlopen.
        Returns None if exception.
        '''

        if proxyDict is not None:
            proxy_support = request.ProxyHandler(proxyDict)
            opener = request.build_opener(proxy_support)
            request.install_opener(opener)

        urlResponse = None

        try:
            urlResponse = request.urlopen(self.__urlBeingCrawled)

        except error.URLError as e:
            err_string = ("Error for url " + self.__urlBeingCrawled)
            if hasattr(e, "reason"):
                err_string += "%s" %(e.reason)
            if hasattr(e, "code"):
                err_string += ("The server couldn't fulfill the request")
                err_string += ("Error code: %s" % e.code)
            logging.error(err_string)
        return urlResponse

    def __decodeResponse(self, responseObject, charset='utf-8'):

        if responseObject is None:
            raise ValueError("responseObject is None!")

        htmlBytes = responseObject.read()
        htmlString = htmlBytes.decode(charset)

        return htmlString

    def __storeWebPageToFile(self, htmlString, nameOfFile, encoding):

        if htmlString is None:
            raise ValueError("htmlString is None!")

        f = open(nameOfFile, "w", encoding=encoding)
        f.write(htmlString)
        f.close()

    def __findOutgoingURLs(self, url, htmlString):

        urlFinderObj = UrlFinder(url)
        urlFinderObj.feed(htmlString)
        return urlFinderObj.getSetOfURLs()


def executeCrawler(urlToCrawl, threadReturnDataDict, thread_id,
                   proxyDict=None):
    '''
    Initializes and executes a crawler to get webpage, and stores it to file.
    Adds htmlString and setOfURLs to their dicts.
    '''

    crawlerObj = Crawler()
    crawlerObj.assignUrl(urlToCrawl)
    htmlString, setOfURLs = crawlerObj.crawlUrl(proxyDict,
                                                storeToFileFlag=True)

    if setOfURLs is not None and htmlString is not None:
        
        threadReturnDataDict[thread_id] = {}
        threadReturnDataDict[thread_id][SetOfURLs_KEY] = setOfURLs
        threadReturnDataDict[thread_id][HTMLString_KEY] = htmlString
