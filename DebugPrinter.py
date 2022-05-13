import json
from threading import Lock

class JsonPrinter():
    
    def __init__(self):
        self._printLock = Lock()
    
    def printJson(self, link:str, timestamp:float, title:str, text:str):
        jsonOut = {"URL": link,
                    "Title": title,
                    "Text": text,
                    "Timestamp":timestamp}
        
        self._printLock.acquire()
        #https://stackoverflow.com/a/18337754/16264901
        print(json.dumps(jsonOut, ensure_ascii=False, indent='\t').encode('utf-8').decode())
        self._printLock.release()
    
    def getJsonOfDict(self, myDict: dict):
        return json.dumps(myDict, ensure_ascii=False, indent='\t').encode('utf-8').decode()