import re
from query import *
def advanced_search(string1,string2,string3,string4,typ,inv_index,url_inv_index,idf,uidf,doc_len,url_len,pr,url_to_terms):

    ind_doc,doc,stringa1=free_text_query(string1,inv_index)
    uind_doc,udoc,stringa1=free_text_query(string1,url_inv_index)
    all_doc1=doc|udoc
    flag=bool(all_doc1)
# checking for documnets containing OR Words
    if (flag==False and stringa1!=''):
        print("There are no files with atleast one of the OR words")
        return [],{},''

    if (string2!=''):
        doc=and_query(string2,inv_index)
        udoc=and_query(string2,url_inv_index)
        all_doc2=doc|udoc
        flag2=bool(all_doc2)
# checking for documnets containing AND Words  
        print(string2)  
        if (flag2==False ):
            print("There are no files with atleast one of the AND words")
            return [],{},''
    print(string3)
    if string3!='':
        doc_p=phrase_query(string3,inv_index)
        if (doc_p==[] ):
            print("No files with given phrase") 
            return [],{},''
# checking for documnets containing phrase words


    ind_doc,doc,stringa1=free_text_query(string4,inv_index)
    uind_doc,udoc,stringa1=free_text_query(string4,url_inv_index)
    all_doc3=doc|udoc
# checking for docs containing NOT words

    string=string1+' '+string2+' '+string3

    ind_doc,doc,string=free_text_query(string,inv_index)
    uind_doc,udoc,string=free_text_query(string,url_inv_index)

    all_doc=doc|udoc
    print(len(all_doc))
    if(stringa1!=''):
        all_doc=all_doc& all_doc1
    if(string2!=''):
        all_doc=all_doc& all_doc2
    if(string3!=''):
        all_doc=all_doc& set(doc_p)
    if(string4!=''):
        all_doc=all_doc- all_doc3
# ensuring that all the condtions are met
    if(bool(all_doc)==False):
        print("NOT files expand all over")
        return [],{},string

    score,x,y,chec={},{},{},{}
    rank=[]
    file_word={}

    for fil in doc|udoc:
        y[fil],x[fil],score[fil],chec[fil]=0,0,0,0
        file_word[fil]=[]

    for word in string.lower().split(): 
        
        for filename in ind_doc[word]:
                temp=len(inv_index[word][filename])
                x[filename]+=temp*(2.5)*idf[word]/(temp+1.5*(0.25+0.75*doc_len[filename]))
                file_word[filename].append(inv_index[word][filename][0])
        for fil in uind_doc[word]:
            temp=len(url_inv_index[word][fil])
            y[fil]+=temp*uidf[word]/url_len[fil]
            chec[fil]+=temp


    for fil in all_doc:
        if chec[fil]>=url_len[fil]-3:
            score[fil]+=5
        score[fil]+=x[fil] + y[fil]
        score[fil]+=pr[int(fil)]
        if typ !='':
            if(typ in url_to_terms[fil] and fil in all_doc):
               rank.append((fil,score[fil],pr[int(fil)]))
        else:
            if(fil in all_doc):
                rank.append((fil,score[fil],pr[int(fil)]))
    return rank,file_word,string

    

