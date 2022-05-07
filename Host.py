import datetime
import logging
from reppy import Robots
from collections import deque

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
        
        if self._robots.agent(HostInfo.AGENTNAME).delay == None:
            return MIN_DELAY_TIME_SECONDS
        else:
            return self._robots.agent(HostInfo.AGENTNAME).delay

    def tryFirstAccessToRobots(self):
        hostRobotsPath = Robots.robots_url(self._hostNameWithSchema)
        try:
            MAX_TIME_REQ_FOR_ROBOTS = 10.0
            hostRobots = Robots.fetch(hostRobotsPath, timeout=MAX_TIME_REQ_FOR_ROBOTS)
        except:
            self._robots = None
            self._couldNotAccessRobots = True
        else:
            self._robots = hostRobots

    def nextRequestAllowedTimestampFromNow(self):
        now = datetime.datetime.now()
        delay = datetime.timedelta(seconds=self.requestDelaySeconds())
        nextAllowedTime = now + delay
        return datetime.datetime.timestamp(nextAllowedTime)

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
    
    def createInfoForHostIfNotExists(self, host:str):
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