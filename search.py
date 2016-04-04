import re

from query import *


def srch(string,inv_index,url_inv_index,idf,uidf,doc_len,url_len,pr):
    pat=re.compile('\"[\w\ ]+\"')
    phr=pat.findall(string)
    

    ind_doc,doc,string=free_text_query(string,inv_index)
    uind_doc,udoc,string=free_text_query(string,url_inv_index)

    all_doc=doc | udoc

    score,x,y,chec={},{},{},{}
    rank=[]
    file_word={}
 

    for fil in all_doc:
        y[fil],x[fil],score[fil],chec[fil]=0,0,0,0
        file_word[fil]=[]

    pattern = re.compile('[\W_]+')
   
    for phrase in phr:
        ph = pattern.sub(' ',phrase)
        temp=phrase_query(phrase,inv_index)
        if temp==[]:
            return [],{},string
        for fil in temp :
            score[fil]+=10+0.1*len(ph.split())
    
    for word in string.lower().split(): 
        
        for filename in ind_doc[word]:
                temp=len(inv_index[word][filename])
                x[filename]+=temp*(2.5)*idf[word]/(temp+1.5*(0.25+0.75*doc_len[filename]))
# computing bm25 - [f(q)*k*idf(q)]/[f(q)+k((1-b)+b(docLength/avg_docLength))]

                file_word[filename].append(inv_index[word][filename][0])
        for fil in uind_doc[word]:
            temp=len(url_inv_index[word][fil])
            y[fil]+=temp*uidf[word]/url_len[fil]
            chec[fil]+=temp

    for fil in all_doc:
        if chec[fil]>=url_len[fil]-3:
            score[fil]+=5
# to check if url was searched            
        score[fil]+=x[fil] + 0.1 * y[fil]
        score[fil]+=pr[int(fil)]
        rank.append((fil,score[fil],pr[int(fil)],x[fil],y[fil]))
    return rank,file_word,string


