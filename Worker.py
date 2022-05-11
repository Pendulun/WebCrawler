from urllib3.exceptions import NewConnectionError, TimeoutError, MaxRetryError
from WorkersPipeline import WorkersPipeline
from WebAccesser import WebAccesser
from queue import PriorityQueue
from bs4 import BeautifulSoup
import datetime
import logging
import Parser
import utils
import time
import Host

class UnwantedPagesHeuristics():
    UNWANTEDDOCTYPESTHREECHARS = set(["pdf", "csv", "png", "svg", "jpg", "gif", "raw","cr2",
                                        "nef", "orf", "sr2", "bmp", "tif", "ogv"])
    
    UNWANTEDDOCTYPESFOURCHARS = set(["tiff", "jpeg"])

    @staticmethod
    def passHeuristicsAccess(url:str) -> bool:
        passThreeChars = True
        passFourChars = True

        if len(url) > 3:
            passThreeChars = url[-3:] not in UnwantedPagesHeuristics.UNWANTEDDOCTYPESTHREECHARS
        
        if len(url) > 4:
            passFourChars = url[-4:] not in UnwantedPagesHeuristics.UNWANTEDDOCTYPESFOURCHARS

        return all([passThreeChars, passFourChars])

class Worker():

    """
    This is a worker that effectively crawls web pages
    """

    MAXPRIORITYFORHOST = 0
    REQ_HEADERS = {'User-Agent': "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion"}
    
    def __init__(self, id):
        #Worker Id
        self._id = id

        #Communicator between workers
        self._workersPipeline = WorkersPipeline({}, 0)

        #Next host to request from
        self._hostsQueue = PriorityQueue()

        #Set of hosts currently on the _hostsQueue
        self._hostsOnQueue = set()

        #All hosts discovered with their policies
        self._hostsInfo = Host.HostsInfo()
    
    @property
    def id(self) -> int:
        return self._id
    
    @id.setter
    def id(self, newId):
        raise AttributeError("id is not writable")
    
    @property
    def hostsInfo(self) -> int:
        raise AttributeError("hostsInfo is not writable")
    
    @hostsInfo.setter
    def hostsInfo(self, newHostsInfo):
        raise AttributeError("hostsInfo is not writable")

    @property
    def workersPipeline(self):
        raise AttributeError("workersPipeline is not readable")
    
    @workersPipeline.setter
    def workersPipeline(self, newWorkersPipeline: WorkersPipeline):
        self._workersPipeline = newWorkersPipeline

    @property
    def hostsQueue(self):
        raise AttributeError("hostsQueue is not readable")
    
    @hostsQueue.setter
    def hostsQueue(self, newHostsQueue):
        raise AttributeError("hostsQueue is not writable")
    
    @property
    def totalPagesCrawled(self) -> int:
        return self._hostsInfo.getTotalNumCrawledResources()
    
    @totalPagesCrawled.setter
    def totalPagesCrawled(self, newTotalPagesCrawled):
        raise AttributeError("totalPagesCrawled is not writable")
    
    def addAllLinksToRequest(self, links):
        for link in links:
            self.addLinkToRequest(link)

    def addLinkToRequest(self, newLink:str):
        hostWithSchema, resources = utils.getHostWithSchemaAndResourcesFromLink(newLink)

        if not self._hostsInfo.alreadyCrawled(hostWithSchema, resources):
            
            firstTimeHost = False
            if not self._hostsInfo.hostExists(hostWithSchema):
                firstTimeHost = True
                self._hostsInfo.createInfoForHostIfNotExists(hostWithSchema)

            self._putResourceIntoResourcesQueueOfHost(hostWithSchema, resources)

            if hostWithSchema not in self._hostsOnQueue:
                if firstTimeHost:
                    self._addHostWithMaxPriorityToRequest(hostWithSchema)
                else:
                    hostInfo = self._hostsInfo.getHostInfo(hostWithSchema)
                    self._addHostToRequest(hostWithSchema, hostInfo.nextRequestAllowedTimestampFromNow())
    
    def _putResourceIntoResourcesQueueOfHost(self, host:str, resource:str):
        hostInfo = self._hostsInfo.getHostInfo(host)
        hostInfo.addResource(resource)
    
    def _addHostWithMaxPriorityToRequest(self, host:str):
        self._addHostToRequest(host, Worker.MAXPRIORITYFORHOST)
    
    def _addHostToRequest(self, host:str, priority:int):
        if host not in self._hostsOnQueue:
            self._hostsOnQueue.add(host)
            self._hostsQueue.put((priority, host))

    def crawl(self):

        allWorkersFinished = False
        webAccess = WebAccesser()

        while not allWorkersFinished:

            self._crawlUntilItCan(webAccess)
            
            self._workersPipeline.waitForLinkOrAllDoneEvent(self._id)

            if self._workersPipeline.allDone:
                logging.info("Terminou Operações")
                allWorkersFinished = True
            else:
                self._tryToCompleteWithReceivedLinks()
        
        self._workersPipeline.setSaiu(self._id)
        logging.info(f"NAO SAIRAM:\n{self._workersPipeline.getNaoSairam()}")

    def _crawlUntilItCan(self, webAccess:WebAccesser):
        
        shouldCheckForOtherLinksCount = 0
        CHECK_FOR_OTHER_LINKS_EVERY_NUM_REQUESTS = 15

        while self._hasLinkToRequest() and not self._workersPipeline.allDone:
            
            completeLink, minTimestampToReq = self._getNextLinkAndMinTimestampToRequest()
            logging.info(f"MIN_DELAY_CURR_PAGE:{minTimestampToReq} URL:{completeLink}")
            currHostWithSchema = utils.getHostWithSchemaOfLink(completeLink)
            hostInfo = self._hostsInfo.getHostInfo(currHostWithSchema)

            self._requestForRobotsOfHostIfNecessary(hostInfo, webAccess)

            if self._shouldAccessPage(completeLink, hostInfo):
                
                self._waitMinDelayIfNecessary(minTimestampToReq)

                self._accessPageAndGetLinks(webAccess, completeLink, hostInfo)

                if not hostInfo.emptyOfResources():
                    self._addHostToRequest(hostInfo.hostNameWithSchema, hostInfo.nextRequestAllowedTimestampFromNow())
            else:
                logging.info(f"should not access page")

            hostInfo.markResourceAsCrawled(utils.getResourcesFromLink(completeLink))
            shouldCheckForOtherLinksCount+=1

            if shouldCheckForOtherLinksCount == CHECK_FOR_OTHER_LINKS_EVERY_NUM_REQUESTS:
                self._tryToCompleteWithReceivedLinks()
                shouldCheckForOtherLinksCount = 0

    def _waitMinDelayIfNecessary(self, minTimestampToReq):
        now = datetime.datetime.now()
        minTimeToWait = datetime.datetime.fromtimestamp(minTimestampToReq)

        if minTimeToWait > now:
            
            timeDiff = minTimeToWait - now
            time.sleep(timeDiff.total_seconds())
    
    def _hasLinkToRequest(self) -> bool:
        return not self._hostsQueue.empty()
    
    def _getNextLinkAndMinTimestampToRequest(self):
        minTimestamp, nextHost = self._getNextHostToRequest()
        nextHostResource = self._getNextResourceToRequestOfHost(nextHost)
        completeLink = utils.getCompleteLinkFromHostAndResource(nextHost, nextHostResource)
        return completeLink, minTimestamp
    
    def _getNextHostToRequest(self) -> str:
        return self._hostsQueue.get()

    def _getNextResourceToRequestOfHost(self, host:str) -> str:
        hostInfo = self._hostsInfo.getHostInfo(host)
        return hostInfo.getNextResource()

    def _requestForRobotsOfHostIfNecessary(self, hostInfo:Host.HostInfo, webAcess:WebAccesser):
        if not hostInfo.hasRobots():
            hostInfo.tryFirstAccessToRobots(webAcess)
    
    def _shouldAccessPage(self, completeLink:str, hostInfo:Host.HostInfo) -> bool:

        allowed = hostInfo.canAccessPage(completeLink)

        passHeuristics = UnwantedPagesHeuristics.passHeuristicsAccess(completeLink)

        return allowed and passHeuristics

    def _accessPageAndGetLinks(self, webAccess:WebAccesser, requestLink:str, hostInfo:Host.HostInfo):
        currHostWithSchema = hostInfo.hostNameWithSchema

        try:
            webAccess.GETRequest(requestLink)

        except Exception as e:
            pass
        else:

            if webAccess.lastRequestSuccess() and webAccess.lastResponseHasTextHtmlContent():
                
                parsedHTML = Parser.HTMLParser.parseHTMLBytes(webAccess.lastResponseTextBytes())

                treatedUrls = self._getTreatedLinksFromPage(parsedHTML, currHostWithSchema)

                self._distributeUrlsToWorkers(treatedUrls)

                response = webAccess.lastResponse
                self._workersPipeline.saveResponse(response, requestLink)
                
                reqTimestamp = webAccess.lastRequestTimestamp
                self._workersPipeline.printIfOnDebugMode(requestLink, reqTimestamp, parsedHTML)

    def _getTreatedLinksFromPage(self, parsedHTML:BeautifulSoup, currHostWithSchema:str):
        urlsFound = Parser.HTMLParser.getAllLinksFromParsedHTML(parsedHTML)
        treatedUrls = Parser.HTMLParser.formatUrlsWithHostIfNeeded(urlsFound, currHostWithSchema)
        return treatedUrls

    def _distributeUrlsToWorkers(self, treatedUrls):
        linksByWorker = self._workersPipeline.separateLinksByWorker(treatedUrls)
                            
        myLinks = linksByWorker[self._id]
        self.addAllLinksToRequest(myLinks)

        linksByWorker.pop(self._id, None)
        self._workersPipeline.sendLinksToProperWorkers(linksByWorker)
    
    def _tryToCompleteWithReceivedLinks(self):
        
        linksWorkersSentToMe = self._workersPipeline.getLinksSentToWorker(self._id)

        while len(linksWorkersSentToMe) > 0:
            newLink = linksWorkersSentToMe.popleft()
            self.addLinkToRequest(newLink)
    
    def getCrawlingInfo(self) -> str:
        hostsOnQueue = [host for host in self._hostsOnQueue]
        requestsMade = self._hostsInfo.getCrawledResourcesPerHost()
        requestsToBeDone = str(self._hostsInfo)

        return f"Hosts on Queue:\n{hostsOnQueue}\nRequests to be Done:\n{requestsToBeDone}\nRequests Made:\n{requestsMade}"