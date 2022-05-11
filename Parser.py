from charset_normalizer import from_bytes
from bs4 import BeautifulSoup
from bs4.element import Comment
import utils

class HTMLParser():
    
    @staticmethod
    def parseHTMLBytes(html:str) -> BeautifulSoup:
        html = str(from_bytes(html).best())
        return BeautifulSoup(html, features="html.parser")
    
    @staticmethod
    def getAllLinksFromParsedHTML(parsedHTML:BeautifulSoup) -> set:
        
        allAnchorsFound = parsedHTML.find_all("a")

        urlsFound = set()
        for anchorTag in allAnchorsFound:
            href = anchorTag.get("href")
            if href != None and href.strip() != "":
                urlsFound.add(href.split()[0])
        
        return urlsFound
    
    @staticmethod
    def formatUrlsWithHostIfNeeded(urls, host:str) -> set:
        formatedUrls = set()

        for url in urls:
            if url != None and url.strip() != "":
                if url[0] != "#":
                    url = url.split("#")[0]
                    formatedUrl = ""

                    if url[0] == "/":
                        formatedUrl = f"{host}{url}"
                    elif (len(url) >= 4 and url[:4] == "http") or (len(url) >= 5 and url[:5] == "https"):
                        formatedUrl = url

                    if(formatedUrl != ""):
                        formatedUrls.add(utils.normalizeLinkIfCan(formatedUrl))
        
        return formatedUrls

    @staticmethod
    def getNFirstTextWords(parsedHTML: BeautifulSoup, numWords:int) -> str:
        allText = HTMLParser.getVisibleTextFromParsedHtml(parsedHTML)
        splitedText = allText.split()
        if len(splitedText) < numWords:
            return " ".join(splitedText)
        else:
            return " ".join(splitedText[:numWords])
    
    @staticmethod
    def getVisibleTextFromParsedHtml(parsedHTML: BeautifulSoup) -> str:
        """
        https://stackoverflow.com/a/1983219/16264901
        """
        texts = parsedHTML.findAll(text=True)
        visible_texts = filter(HTMLParser.tag_visible, texts)  
        return " ".join(text.strip() for text in visible_texts)
    
    @staticmethod
    def tag_visible(element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True
