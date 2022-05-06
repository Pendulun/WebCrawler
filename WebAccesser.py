import logging
import urllib3
import certifi

class WebAccesser():

    REQ_HEADERS = {'User-Agent': "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion"}

    def __init__(self):
        self._poolManager = self._getCustomPoolManager()
        self._lastResponse = None
    
    @property
    def poolManager(self):
        raise AttributeError("poolManager is not directly readable")
    
    @poolManager.setter
    def poolManager(self, newPool):
        raise AttributeError("poolManager is not directly writable")
    
    @property
    def lastResponse(self):
        raise AttributeError("lastResponse is not directly readable")
    
    @lastResponse.setter
    def lastResponse(self, newPool):
        raise AttributeError("lastResponse is not directly writable")

    def _getCustomPoolManager(self):
        customRetries = urllib3.util.Retry(connect=1, read=2, redirect=5)
        timeout = urllib3.util.Timeout(connect=2.0, read=2.0)
        return urllib3.PoolManager(
                                    retries=customRetries,
                                    cert_reqs='CERT_REQUIRED',
                                    ca_certs=certifi.where(),
                                    timeout=timeout
                                )
    
    def GETRequest(self, link:str):
        self._lastResponse = self._poolManager.request('GET', link, headers=WebAccesser.REQ_HEADERS)
    
    def lastResponseText(self) -> str:
        if self._lastResponse != None:
            return self._lastResponse.data
        else:
            return None
    
    def lastRequestSuccess(self) -> bool:
        if self._lastResponse != None:
            return self._lastResponse.status >= 200 and self._lastResponse.status < 300 
        else:
            return False
    
    #httpResponse é do tipo urllib3.response.HTTPResponse
    #https://urllib3.readthedocs.io/en/stable/reference/urllib3.response.html?highlight=HTTPResponse#urllib3.response.HTTPResponse
    def lastResponseHasTextHtmlContent(self) -> bool:
        if self._lastResponse == None:
            return False
        else:
            return 'text/html' in self._lastResponse.headers['content-type']