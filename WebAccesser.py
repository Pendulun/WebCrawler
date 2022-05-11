import datetime
import logging
import urllib3
import certifi
import reppy
import utils

class WebAccesser():

    REQ_HEADERS = {'User-Agent': "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion"}

    def __init__(self):
        self._poolManager = self._getCustomPoolManager()
        self._lastResponse = None
        self._lastRequestTimestamp = 0.0
        logging.getLogger("urllib3").setLevel(logging.CRITICAL)
    
    @property
    def poolManager(self):
        raise AttributeError("poolManager is not directly readable")
    
    @poolManager.setter
    def poolManager(self, newPool):
        raise AttributeError("poolManager is not directly writable")

    @property
    def lastRequestTimestamp(self) -> float:
        return self._lastRequestTimestamp
    
    @lastRequestTimestamp.setter
    def lastRequestTimestamp(self, newLastRequestTimestamp):
        raise AttributeError("lastRequestTimestamp is not directly writable")
    
    @property
    def lastResponse(self) -> urllib3.response.HTTPResponse:
        return self._lastResponse
    
    @lastResponse.setter
    def lastResponse(self, newPool):
        raise AttributeError("lastResponse is not directly writable")

    def _getCustomPoolManager(self):
        timeout = urllib3.util.Timeout(connect=2.0, read=3.0)
        return urllib3.PoolManager(
                                    retries=False,
                                    cert_reqs='CERT_REQUIRED',
                                    ca_certs=certifi.where(),
                                    timeout=timeout
                                )
    
    def getRobotsOf(self, url:str) -> reppy.Robots:
        hostRobotsPath = reppy.Robots.robots_url(utils.normalizeLinkIfCan(url))

        hostRobots = None
        MAX_TIME_REQ_FOR_ROBOTS = 10.0
        try:
            hostRobots = reppy.Robots.fetch(hostRobotsPath, 
                                            timeout=MAX_TIME_REQ_FOR_ROBOTS,
                                            headers=WebAccesser.REQ_HEADERS)
        except:
            hostRobots = None
        
        return hostRobots

    def GETRequest(self, link:str):
        now = datetime.datetime.now()
        self._lastRequestTimestamp = datetime.datetime.timestamp(now)
        self._lastResponse = self._poolManager.request('GET', link, headers=WebAccesser.REQ_HEADERS)
    
    def lastResponseTextBytes(self) -> bytes:
        if self._lastResponse != None:
            return self._lastResponse.data
        else:
            return None
    
    def lastRequestSuccess(self) -> bool:
        if self._lastResponse != None:
            return self._lastResponse.status >= 200 and self._lastResponse.status < 300 
        else:
            return False
    
    def lastResponseHasTextHtmlContent(self) -> bool:
        if self._lastResponse == None:
            return False
        else:
            return 'text/html' in self._lastResponse.getheader('content-type', "")