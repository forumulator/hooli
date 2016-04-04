from bs4 import BeautifulSoup


def correction(html):
    soup=BeautifulSoup(html)
        
    for tag in ['script','style']:
        for s in soup.find_all(tag):
            s.replaceWith(" ")
#removing script and style tags
    html_title=''
    for sent in soup.find_all('title'):
        sp=BeautifulSoup(str(sent))
        sent=sp.get_text()
        html_title=sent  
 # obtaining title of html page            

    try:
        ht=str(soup.prettify())
# this is to prevent words from glueing together after tags are removed
        soup2=BeautifulSoup(ht)
        return str(soup2.get_text()),html_title
# content extracted

    except:
# the prettify() fn goes into infinte recursion when <TABLE> is present        
        return str(soup.get_text()),html_title
# content extracted

