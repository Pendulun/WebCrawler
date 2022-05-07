from bs4 import BeautifulSoup
from url_normalize import url_normalize
from charset_normalizer import from_bytes

class HTMLParser():
    
    def __init__(self):
        self._parsedHTML = None

    @property
    def parsedHTML(self):
        raise AttributeError("parsedHTML is not directly readable")

    @parsedHTML.setter
    def parsedHTML(self, newParsedHTML):
        raise AttributeError("parsedHTML is not directly writable")
        
    def parse(self, html:str):
        html = str(from_bytes(html).best())
        self._parsedHTML = BeautifulSoup(html, features="html.parser")
    
    def getAllLinksFromParsedHTML(self) -> set:
        
        allAnchorsFound = self._parsedHTML.find_all("a")

        urlsFound = set()
        for anchorTag in allAnchorsFound:
            urlsFound.add(anchorTag.get("href"))
        
        return urlsFound
    
    def formatUrlsWithHostIfNeeded(self, urls, host:str) -> set:
        formatedUrls = set()

        for url in urls:
            if url != None and url.strip() != "":
                if url[0] != "#":
                    formatedUrl = ""

                    if url[0] == "/":
                        formatedUrl = f"{host}{url}"
                    elif (len(url) >= 4 and url[:4] == "http") or (len(url) >= 5 and url[:5] == "https"):
                        formatedUrl = url

                    if(formatedUrl != ""):
                        formatedUrls.add(url_normalize(formatedUrl))
        
        return formatedUrls
