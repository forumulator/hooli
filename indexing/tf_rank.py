# takes index and adds the weights
import re
from bs4 import *
def tf(index):
    pattern=re.compile('[\W_]+')
    title={}
    ctr=0
    for line in open('All pages.txt','r'):
        filename=line.split()[0].replace('.html','')
        f=open('HTML/'+filename+'.html','r',encoding="utf-8")   
        content=f.read()
        f.close()
        ctr+=1
        if(ctr%1000==0):
            print(ctr)
        soup=BeautifulSoup(content)
        for sent in soup.find_all('title'):
            sp=BeautifulSoup(str(sent))
            sent=sp.get_text()
            title[filename]=sent
            sent=pattern.sub(' ',sent)
	
            for word in sent.lower().split():
                try:                
                    poslist=index[filename][word]
                    if type(poslist) is list:
                        index[filename][word]=(poslist,6.5)
                    else:
                        n=index[filename][word][1]
                        n=n+6.5
                        index[filename][word]=(poslist[0],n)
                except:
                    pass
        soup=BeautifulSoup(content.lower())
        try:
            soup=BeautifulSoup(soup.prettify())
        except:
            pass

        for sent in soup.find_all('h1'):
            sp=BeautifulSoup(str(sent))
            sent=sp.get_text()
            sent=pattern.sub(' ',sent)
	
            for word in sent.lower().split():
                try:                
                    poslist=index[filename][word]
                    if type(poslist) is list:
                        index[filename][word]=(poslist,4.5)
                    else:
                        n=index[filename][word][1]
                        n=n+4.5
                        index[filename][word]=(poslist[0],n)
                except:
                    pass

    for sent in soup.find_all('h2'):
            sp=BeautifulSoup(str(sent))
            sent=sp.get_text()
            sent=pattern.sub(' ',sent)
	
            for word in sent.lower().split():
                try:                
                    poslist=index[filename][word]
                    if type(poslist) is list:
                        index[filename][word]=(poslist,3)
                    else:
                        n=index[filename][word][1]
                        n=n+3
                        index[filename][word]=(poslist[0],n)
                except:
                    pass

    for sent in soup.find_all(['em','i','b']):
            sp=BeautifulSoup(str(sent))
            sent=sp.get_text()
            sent=pattern.sub(' ',sent)
	
            for word in sent.lower().split():
                try:                
                    poslist=index[filename][word]
                    if type(poslist) is list:
                        index[filename][word]=(poslist,1.5)
                    else:
                        n=index[filename][word][1]
                        n=n+1.5
                        index[filename][word]=(poslist[0],n)
                except:
                    pass

    return index,title
        
