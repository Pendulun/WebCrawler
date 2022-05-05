from datetime import datetime
import logging
from reppy import Robots
from collections import deque
import requests
from bs4 import BeautifulSoup

class HostInfo():
    
    AGENTNAME = '*'
    MAXNUMINNERSITEMAPSCRAWLABLE = 5

    def __init__(self, hostWithSchema):
        self._resourcesQueue = deque()
        self._robots = None
        self._couldNotAccessRobots = False
        self._hostNameWithSchema = hostWithSchema
        self._crawledResources = set()
    
    @property
    def hostNameWithSchema(self):
        return self._hostNameWithSchema
    
    @hostNameWithSchema.setter
    def hostNameWithSchema(self, newHostName):
        raise AttributeError("hostNameWithSchema is not directly writable")
    
    @property
    def crawledResources(self):
        raise AttributeError("crawledResources is not directly readable")
    
    @crawledResources.setter
    def crawledResources(self, newCrawledResources):
        raise AttributeError("crawledResources is not directly writable")
    
    @property
    def couldNotAccessRobots(self):
        raise AttributeError("couldNotAccessRobots is not directly readable")
    
    @couldNotAccessRobots.setter
    def couldNotAccessRobots(self, newHostName):
        raise AttributeError("couldNotAccessRobots is not directly writable")
    
    @property
    def resourcesQueue(self):
        raise AttributeError("resourcesQueue is not directly readable")
    
    @resourcesQueue.setter
    def resourcesQueue(self, newResourcesQueue):
        raise AttributeError("resourcesQueue is not directly writable")
    
    @property
    def robots(self):
        raise AttributeError("robots is not directly readable")
    
    @robots.setter
    def robots(self, newRobots):
        raise AttributeError("robots is not directly writable")
    
    def addResource(self, resource:str):
        logging.info(f"Adicionando {self._hostNameWithSchema}{resource}")
        self._resourcesQueue.append(resource)
    
    def addResources(self, newResources):
        self._resourcesQueue.extend(newResources)
    
    def getNextResource(self) -> str:
        if not self.emptyOfResources():
            return self._resourcesQueue.popleft()
        else:
            return None
    
    def emptyOfResources(self) -> bool:
        return len(self._resourcesQueue) == 0

    def hasRobots(self) -> bool:
        if self._couldNotAccessRobots:
            return True
        
        if self._robots == None:
            return False
        else:
            return True
    
    def canAccessPage(self, completePageLink:str) -> bool:
        if self._couldNotAccessRobots:
            return True
        
        if self._robots == None:
            self.tryFirstAccessToRobots()
        
            if self._robots == None:
                return True

        return self._robots.allowed(completePageLink, HostInfo.AGENTNAME)
    
    def requestDelaySeconds(self) -> float:
        MIN_DELAY_TIME_SECONDS = 0.1

        if self._couldNotAccessRobots:
            return MIN_DELAY_TIME_SECONDS
        
        if self._robots == None:
            self.tryFirstAccessToRobots()
        
            if self._robots == None:
                return MIN_DELAY_TIME_SECONDS
        
        return self._robots.agent(HostInfo.AGENTNAME).delay

    def tryFirstAccessToRobots(self):
        hostRobotsPath = Robots.robots_url(self._hostNameWithSchema)
        try:
            hostRobots = Robots.fetch(hostRobotsPath)
        except:
            self._robots = None
            self._couldNotAccessRobots = True
        else:
            self._robots = hostRobots
    
    def saveLinksFromSitemapIfPossible(self):
        if self.hasRobots():
            sitemapListOnRobots = self._robots.sitemaps

            if len(sitemapListOnRobots) > 0:
                
                firstSitemap = sitemapListOnRobots[0]
                allLinksFound = self._findMaxLinksPossible(firstSitemap)

                self.addResources(allLinksFound)
    
    def _findMaxLinksPossible(self, sitemapLink:str) -> list:
        sitemapPage = requests.get(sitemapLink)
        sitemapXml = sitemapPage.text
        sitemapSoup = BeautifulSoup(sitemapXml)

        pagesInThisSitemap = self._findPagesOnSitemapSoup(sitemapSoup)
        pagesInInnerSitemaps = self._findLinksFromInnerSitemaps(sitemapSoup)

        allLinksFound = list() 
        allLinksFound.extend(pagesInThisSitemap)
        allLinksFound.extend(pagesInInnerSitemaps)
    
    def _findLinksFromInnerSitemaps(self, firstSitemapSoup) -> list:

        linkToPagesFound = list()
        sitemapTags = firstSitemapSoup.find_all("sitemap") 
        if len(sitemapTags) > 0:
            
            otherSitemaps = list()    
            for innerSitemap in sitemapTags:
                otherSitemaps.append(innerSitemap.findNext("loc").text)

            if len(otherSitemaps) > HostInfo.MAXNUMINNERSITEMAPSCRAWLABLE:
                otherSitemaps = otherSitemaps[:HostInfo.MAXNUMINNERSITEMAPSCRAWLABLE]
                     
            linkToPagesFound.extend(self._findLinksOnSitemaps(otherSitemaps))
        
        return linkToPagesFound

    def _findLinksOnSitemaps(self, sitemapsList:list) -> list:
        linksFound = list()

        for sitemapLink in sitemapsList:
            sitemapPage = requests.get(sitemapLink)
            sitemapXML = sitemapPage.text

            sitemapSoup = BeautifulSoup(sitemapXML)
            linksFound = self._findPagesOnSitemapSoup(sitemapSoup)
            linksFound.extend(linksFound)
        
        return linksFound
    
    def _findPagesOnSitemapSoup(self, sitemapSoup):
        linksToPagesFound = list()
        pageLinksFound = sitemapSoup.find_all("url")

        for page in pageLinksFound:
            pageLink = page.findNext("loc").text
            linksToPagesFound.append(pageLink)
        
        return linksToPagesFound

    def nextRequestAllowedTimestampFromNow(self):
        now = datetime.now()
        delay = datetime.timedelta(seconds=self.requestDelaySeconds())
        nextAllowedTime = now + delay
        return datetime.timestamp(nextAllowedTime)

    def getRequestsString(self) -> str:
        return str(self._resourcesQueue)
    
    def markResourceAsCrawled(self, resource:str):
        self._crawledResources.add(resource)
    
    def dismarkResourceAsCrawled(self, resource:str):
        self._crawledResources.discard(resource)
    
    def alreadyCrawledResource(self, resource: str) -> bool:
        return resource in self._crawledResources
    
    def getCrawledResourcesString(self):
        return str(self._crawledResources)
    
    def getCrawledResourcesNum(self):
        return len(self._crawledResources)

class HostsInfo():
    def __init__ (self):
        self._hosts = dict()
    
    @property
    def hosts(self):
        raise AttributeError("hosts is not readable")
    
    @hosts.setter
    def hosts(self, newHosts):
        raise AttributeError("hosts is not writable")
    
    def hostExists(self, host:str) -> bool:
        return host in list(self._hosts.keys())
    
    def getHostInfo(self, host:str) -> HostInfo:
        if self.hostExists(host):
            return self._hosts[host]
        return None
    
    def createInfoForHostIfNotExists(self, host):
        if not self.hostExists(host):
            self._hosts[host] = HostInfo(host)
    
    def getCrawledResourcesPerHost(self) -> str:
        crawled = ""

        for host, hostInfo in self._hosts.items():
            crawled+=f"({host}):{hostInfo.getCrawledResourcesString()} "
        
        return crawled
    
    def alreadyCrawled(self, host:str, resource:str) -> bool:
        if self.hostExists(host):
            return self.getHostInfo(host).alreadyCrawledResource(resource)
        else:
            return False
    
    def getTotalNumCrawledResources(self):
        total = 0

        for _, hostInfo in self._hosts.items():

            total+=hostInfo.getCrawledResourcesNum()
        
        return total
    
    def __str__(self) -> str:
        myStringRep = "HostsInfo(\n"

        for hostName, hostInfo in self._hosts.items():
            myStringRep+=f"({hostName}) : {hostInfo.getRequestsString()}\n"
        
        myStringRep+=")"
        return  myStringRep