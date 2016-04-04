from format_analyzer import *
import re
from text_filtering import *
from idf_calc import *
import sys
import pickle
from update_len import *

number_of_links, number_of_files = 0,0
file_to_terms , url_to_terms , url_mapper = {},{},{}
pattern = re.compile('[\W_]+')
doc_len,url_len,html_title={},{},{}

print("HEY")
for line in open('links.txt','r'):
    temp=line.split()
    doc,url=temp[0],temp[1]

    try:
    	typ=temp[2]

    except:
        pass
# converting url_link to bag of words for indexing
    url_mapper[doc]=url
    url_to_terms[doc]=url.lower()+' '+typ.lower()
    url_to_terms[doc] = pattern.sub(' ',url_to_terms[doc])
    re.sub(r'[\W_]+','', url_to_terms[doc])
    url_to_terms[doc] = url_to_terms[doc].split()
#    

    url_len[doc]=len(url_to_terms[doc])
    number_of_links=number_of_links+1
print("LINKS DONE")
    

for line in open('All pages.txt','r'):
    filename=line.split()[0].replace('.html','')
    f=open('HTML/'+filename+'.html',"r",encoding="utf-8")
    if(f.readline()!=''):
# safe check for empty files
        f.seek(0,0)
        number_of_files=number_of_files+1
# increase the counter for number of files
        f.readline()
        if(number_of_files%1000==0):
            print(number_of_files)    
# to remove the url of page
        html=f.read()
        html,html_title[filename] = correction(html)
# html tags have been stripped   

# converting html page to bag of words
        file_to_terms[filename] = html.lower()
        file_to_terms[filename] = pattern.sub(' ',file_to_terms[filename])
        re.sub(r'[\W_]+','', file_to_terms[filename])
        file_to_terms[filename] = file_to_terms[filename].split()

        doc_len[filename]=len(file_to_terms[filename])
    f.close()


for line in open("All pdf.txt",'r'):
    filename=line.split()[0].replace('.txt','')
    f=open('PDF/'+filename+'.txt','r')
    number_of_files+=1
    x=f.read().lower()
    if(number_of_files%1000==0):
            print(number_of_files)    

    x=pattern.sub(' ',x)
    x=x.split()
    size=min(len(x),40)
    file_to_terms[filename]=x[0:size]
    file_to_terms[filename].append('pdf')
    doc_len[filename]=len(file_to_terms[filename])+1        

print("FILE TO TERMS READY")
# creating indexes of url links and html contents separately
url_index=make_indices(url_to_terms)
index=make_indices(file_to_terms)

url_inv_index=fullIndex(url_index)
inv_index=fullIndex(index)
print(sys.getsizeof(inv_index))

idf,uidf=idf(inv_index,number_of_files,url_inv_index,number_of_links)
print("IDF DONE")
# calculating value of length of document by avgerage length of document
doc_len=update(doc_len,file_to_terms,number_of_files)

print("UPDATION DONE")

with open('company_data.pkl', 'wb') as output:
    pickle.dump(inv_index, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(url_mapper, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(file_to_terms, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(url_inv_index, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(doc_len, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(html_title, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(idf, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(uidf, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(url_len, output, pickle.HIGHEST_PROTOCOL)
    pickle.dump(url_to_terms, output, pickle.HIGHEST_PROTOCOL)
		    




