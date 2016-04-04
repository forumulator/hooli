from main import *
import math
import re
from bs4 import BeautifulSoup
import pickle
# careful with the number_of_file


def compute_idf(idf, word_list,number_of_files,inv_index):
    for word in word_list:
        x=float(number_of_files)/len(inv_index[word].keys())
        x=math.log10(x)
        idf.append(x)
    return idf



def weights(soup,vectr,word_list):
 try:
  soup=BeautifulSoup(soup.prettify())
 except:
  pass

 pattern=re.compile('[\W_]+')
    
 for sent in soup.find_all('title'):
  sp=BeautifulSoup(str(sent))
  sent=sp.get_text()
  sent=pattern.sub(' ',sent)
	
  for word in sent.lower().split():
   if word in word_set:
    i=word_list.index(word)
    vectr[i]=vectr[i]+6.5
    
 for sent in soup.find_all('h'):
  sp=BeautifulSoup(str(sent))
  sent=sp.get_text()
  sent=pattern.sub(' ',sent)
	
  for word in sent.lower().split():
   if word in word_set:
    i=word_list.index(word)
    vectr[i]=vectr[i]+4.5
    
 for sent in soup.find_all('h2'):
  sp=BeautifulSoup(str(sent))
  sent=sp.get_text()
  sent=pattern.sub(' ',sent)
	
  for word in sent.lower().split():
   if word in word_set:
    i=word_list.index(word)
    vectr[i]=vectr[i]+3
    
 for sent in soup.find_all(['em','i','b']):
  sp=BeautifulSoup(str(sent))
  sent=sp.get_text()
  sent=pattern.sub(' ',sent)
	
  for word in sent.lower().split():
   if word in word_set:
    i=word_list.index(word)
    vectr[i]=vectr[i]+1.5

 return vectr




word_list=list(inv_index.keys())
# list of all words in html
word_set=set(word_list)

urlw_list=list(url_inv_index.keys())

tf_in={}
# final tf-idf vectors for each document content
utf_in={}
# tf-idf vectors for just url content

idf=[]
# list of idf values for each word
uidf=[]

idf=compute_idf(idf,word_list,number_of_files,inv_index)
uidf=compute_idf(uidf,urlw_list,number_of_links,url_inv_index)

for filename in url_index.keys():
    utf_in[filename]=[]
    s2=0
    for word in urlw_list:
        if filename in url_inv_index[word].keys(): 
            a=len(url_inv_index[word][filename])
        else:
            a=0
        utf_in[filename].append(a)
        s2=s2+a*a

    for i in range(len(urlw_list)):
        utf_in[filename][i]=(utf_in[filename][i]/float(s2))*uidf[i]


for filename in index.keys():
    tf_in[filename]=[]
    
    s=0
    for word in word_list:
        if filename in inv_index[word].keys(): 
            a=len(inv_index[word][filename])
        else:
            a=0
        tf_in[filename].append(a)
        s=s+a
   
    content=open('HTML/'+filename+'.html','r',encoding='utf-8').read().lower()
    soup=BeautifulSoup(content)
    tf_in[filename]=weights(soup,tf_in[filename],word_list)

    for i in range(len(word_list)):
        tf_in[filename][i]=(tf_in[filename][i]/float(s))*idf[i]

print("idf ranking done")

#with open('company_data.pkl', 'wb') as output:
#    pickle.dump(inv_index, output, pickle.HIGHEST_PROTOCOL)
#    pickle.dump(url_mapper, output, pickle.HIGHEST_PROTOCOL)
#    pickle.dump(tf_in, output, pickle.HIGHEST_PROTOCOL)
#    pickle.dump(word_list, output, pickle.HIGHEST_PROTOCOL)



