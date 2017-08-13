from urllib.request import urlopen
from urllib import parse
from linkFinder import linkFinder
from general import *
import hashlib
from bs4 import *


class spider:
    # class variables
    projectName = ''
    baseURL = ''
    domainName = ''
    listFile = ''
    brokenFile = ''
    problemFile = ''
    linkFile = ''
    unknownFile = ''
    graphFile = ''
    skipFile = ''
    queue = set()
    crawled = set()
    hashSet = set()
    counter = 0
    dict = {}

    def __init__(self, projectName, baseURL, domainName):
        spider.projectName = projectName
        spider.baseURL = baseURL
        spider.domainName = domainName
        spider.listFile = spider.projectName + '/list.txt'
        spider.brokenFile = spider.projectName + '/broken.txt'
        spider.problemFile = spider.projectName + '/problem.txt'
        spider.linkFile = spider.projectName + '/link.txt'
        spider.unknownFile = spider.projectName + '/unknown.txt'
        spider.graphFile = spider.projectName + '/graph.txt'
        spider.skipFile = spider.projectName+ '/skip.txt'
        spider.counter = 0

# Creates the directories and files for the Crawler.
        self.boot()

# Crawls the baseURL in the first iteration.
        self.crawlPage(spider.baseURL)

    @staticmethod
    def boot():
        makeDir(spider.projectName)
        createDataFiles(spider.projectName, spider.baseURL)
        spider.queue.add(spider.baseURL)
        spider.dict[spider.baseURL] = spider.counter
        spider.counter += 1

    @staticmethod
    def crawlPage(pageURL):
        if pageURL not in spider.crawled:
            print('Spider crawling ' + pageURL)
            print('Queue ' + str(len(spider.queue)) + ' | Crawled ' + str(len(spider.crawled)))
            try:
                spider.addLinksToQueue(spider.gatherLinks(pageURL), pageURL)
                print('links added')
                spider.crawled.add(pageURL)
            except Exception as e:
                # Logs the error if any
                asdfe = str(e) + ' ' + pageURL
                appendToFile(spider.problemFile, asdfe)

    @staticmethod
    def gatherLinks(pageURL):
        htmlString = ''
        #Checks if the link is in the skipList. If true, then skip the URL
        if spider.skipList(pageURL) == 1:
            content1 = str(spider.dict.get(pageURL)) + ' ' + pageURL
            appendToFile(spider.skipFile,content1)
            return set()
        try:
            response = urlopen(pageURL)
            type = response.getheader('Content-Type')

            #Special code to download the PDF as it is.
            if ".pdf" in pageURL:
                # print('Opened')
                #asd = 'PDFs/' + str(spider.dict.get(pageURL)) + '.pdf'
                #f = open(asd, 'wb')
                #f.write(response.read())
                content = str(spider.dict.get(pageURL)) + ' ' + pageURL + ' ' + type
                appendToFile(spider.linkFile, content)

            # Tries to decode the URL in UTF-8
            elif 'text/html' in type:
                htmlBytes = response.read()
                print('htmlBytes generated')
                try:
                    htmlString = htmlBytes.decode('utf-8')
                    soup=BeautifulSoup(htmlString)
                    htmlText=soup.get_text()
                    print('htmlText done')
                    # Checks if the hash of the URL is already present
                    if spider.checkHash(htmlText.encode('utf-8')) == 1:
                        print('Entered')
                        print('SAME HASH!!' + pageURL)
                        return set()
                    print('Unique URL')
                    asd = 'HTML/' + str(spider.dict.get(pageURL)) + '.html'

                    f = open(asd, 'w', encoding='utf-8')
                    htmlString = pageURL + '\n' + htmlString

                    soup = BeautifulSoup(htmlString)
                    matter = ''
                    for a in soup.find_all('frame', src = True):
                        test = parse.urljoin(pageURL,a['src'])
                        matter = urlopen(test)
                        matter = matter.read()
                        try:
                            matter = matter.decode('ISO-8859-1')
                        except:
                            try:
                                matter = matter.decode('UTF-8')
                            except Exception as e:
                                error = str(e) + str(spider.dict.get(pageURL)) + ' ' + pageURL
                                appendToFile(spider.problemFile, error)
                        htmlString = htmlString + '\n' + matter


                    f.write(htmlString)
                    hashedURL = spider.hashURL(htmlBytes)
                    spider.hashSet.add(hashedURL)
                    content = str(spider.dict.get(pageURL)) + ' ' + pageURL + ' ' + type
                    appendToFile(spider.linkFile, content)

                except Exception as e:
                    #logs the error with the error code
                    asdfg = str(e) + ' ' + str(spider.dict.get(pageURL))+ ' ' + pageURL
                    appendToFile(spider.problemFile, asdfg)
                    return set()

            else:
                # If the URL is not of the above types, then does not do anything
                content = str(spider.dict.get(pageURL)) + ' ' + pageURL + ' ' + type
                appendToFile(spider.linkFile, content)


            # Finds URLs in the page
            finder = linkFinder(spider.baseURL, pageURL)
            finder.feed(htmlString)

        except:
            try:

                response = urlopen(pageURL)
                type = response.getheader('Content-Type')

                # Tries to decode the stream using ISO-8859-1
                if 'text/html' in response.getheader('Content-Type'):
                    htmlBytes = response.read()
                try:
                    htmlString = htmlBytes.decode('iso-8859-1')
                    soup=BeautifulSoup(htmlString)
                    htmlText=soup.get_text()

                    # Checks if the hash of the URL is already present
                    if spider.checkHash(htmlText.encode('utf-8')) == 1:
                        print('SAME HASH!!' + pageURL)
                        return set()

                    path = 'HTML/' + str(spider.dict.get(pageURL)) + '.html'


                    f = open(path, 'w', encoding='utf-8')
                    htmlString = pageURL + '\n' + htmlString


                    soup = BeautifulSoup(htmlString)
                    matter = ''
                    for a in soup.find_all('frame', src = True):
                        test = parse.urljoin(pageURL,a['src'])
                        matter = urlopen(test)
                        matter = matter.read()
                        try:
                            matter = matter.decode('ISO-8859-1')
                        except:
                            try:
                                matter = matter.decode('UTF-8')
                            except Exception as e:
                                error = str(e) + str(spider.dict.get(pageURL)) + ' ' + pageURL
                                appendToFile(spider.problemFile, error)
                        htmlString = htmlString + '\n' + matter


                    f.write(htmlString)

                    # Adds the hash of the page to set
                    hashedURL = spider.hashURL(htmlBytes)
                    spider.hashSet.add(hashedURL)
                    content = str(spider.dict.get(pageURL)) + ' ' + pageURL + ' ' + type
                    appendToFile(spider.linkFile, content)

                except Exception as e:
                    # Log the error and move to the next iteration
                    asd = str(e) + ' ' +str(spider.dict.get(pageURL))+ ' ' +  pageURL
                    appendToFile(spider.problemFile, asd)
                    return set()

                finder = linkFinder(spider.baseURL, pageURL)
                finder.feed(htmlString)

            except Exception as e:
                # Log the error
                asd = str(e) + ' ' + str(spider.dict.get(pageURL))+ ' ' +  pageURL
                appendToFile(spider.brokenFile, asd)
                return set()
        return finder.pageLinks()

    @staticmethod
    def addLinksToQueue(linkSet, pageURL):
        for url in linkSet:
            # Normalizes the URL
            url = urlFix(url)
            # Assigns a number to the URL to use for graph creation
            if url not in spider.dict.keys():
                spider.dict[url] = spider.counter
                spider.counter += 1

            #Adds the directed edge to the list.
            content = str(spider.dict.get(pageURL)) + ' ' + str(spider.dict.get(url))
            appendToFile(spider.graphFile, content)

            # Checks if the URL is in the queue or in crawled list
            if url in spider.queue:
                continue
            if url in spider.crawled:
                continue

            # Checks if the domain is in the URL
            if spider.domainName not in url:
                continue

            spider.queue.add(url)
            appendToFile(spider.listFile, url)

    # To hash the page and check for multiple links pointing to the same content
    @staticmethod
    def hashURL(bytes):
        hashed = hashlib.sha256(bytes)
        hashString = hashed.hexdigest()
        return hashString

    # Boolean function to check equality of hashes
    @staticmethod
    def checkHash(bytes):
        hashedURL = spider.hashURL(bytes)
        if hashedURL in spider.hashSet:
            return 1
        else:
            return 0

    # A list of ignored links, which may cause infinite URLs like calendars.
    # Also links which point outside the intranet but have the domain in its URL
    @staticmethod
    def skipList(pageURL):
        flag = 0
        if 'facebook' in pageURL:
            flag = 1
        elif 'clustrmaps' in pageURL:
            flag = 1
        elif 'jatinga.iitg.ernet.in/cseforum/calendar.php' in pageURL:
            flag = 1
        elif 'intranet.iitg.ernet.in/eventcal/' in pageURL:
            flag = 1
        elif 'www.iitg.ernet.in/pplab/booking/recording.php' in pageURL:
            flag = 1
        elif 'csea.iitg.ernet.in/csea/Public/web_new/index.php/activities/' in pageURL:
            flag = 1
        elif 'shilloi.iitg.ernet.in/~hss/reservation/' in pageURL:
            flag = 1
        elif 'dstats.net' in pageURL:
            flag=1
        elif  '/	form/' in pageURL:
            flag = 1
        elif 'intranet.iitg.ernet.in/news/' in pageURL:
            flag = 1

        # Boolean to check if in skipList
        if flag == 1:
            print('skipped ' + pageURL)
            return 1
        else:
            return 0
