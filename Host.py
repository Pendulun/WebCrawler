from datetime import datetime
from reppy import Robots
from collections import deque

class HostInfo():
    
    AGENTNAME = '*'

    def __init__(self, hostWithSchema):
        self._resourcesQueue = deque()
        self._robots = None
        self._couldNotAccessRobots = False
        self._hostNameWithSchema = hostWithSchema
    
    @property
    def hostNameWithSchema(self):
        return self._hostNameWithSchema
    
    @hostNameWithSchema.setter
    def hostNameWithSchema(self, newHostName):
        raise AttributeError("hostNameWithSchema is not directly writable")
    
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
        return self._resourcesQueue.pop()
    
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
    
    def requestDelay(self) -> int:
        if self._couldNotAccessRobots:
            return 0
        
        if self._robots == None:
            self.tryFirstAccessToRobots()
        
            if self._robots == None:
                return 0
        
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
    
    def nextRequestAllowedTimestampFromNow(self):
        now = datetime.now()
        delay = datetime.timedelta(seconds=self.requestDelay())
        nextAllowedTime = now + delay
        return datetime.timestamp(nextAllowedTime)