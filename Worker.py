from urllib3.exceptions import NewConnectionError, TimeoutError, MaxRetryError
from threading import Lock, Condition
from WorkersPipeline import WorkersPipeline
from WebAccesser import WebAccesser
from collections import deque
from queue import PriorityQueue
from reppy import Robots
import Host
import Parser
import utils
import logging


class UnwantedPagesHeuristics():
    UNWANTEDDOCTYPESTHREECHARS = set(["pdf", "csv", "png", "svg", "jpg", "gif", "raw","cr2",
                                        "nef", "orf", "sr2", "bmp", "tif"])
    
    UNWANTEDDOCTYPESFOURCHARS = set(["tiff", "jpeg"])

    @staticmethod
    def passHeuristicsAccess(url:str) -> bool:
        passThreeChars = url[-3:] not in UnwantedPagesHeuristics.UNWANTEDDOCTYPESTHREECHARS
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
        self._workersPipeline = WorkersPipeline({})

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
        logging.info(f"Hello from Thread {self._id}")

        allWorkersFinished = False
        htmlParser = Parser.HTMLParser()
        webAccess = WebAccesser()

        CHECK_FOR_OTHER_LINKS_EVERY_NUM_REQUESTS = 10
        while not allWorkersFinished:

            shouldCheckForOtherLinksCount = 0
            while self._hasLinkToRequest():

                completeLink = self._getNextLinkToRequest()
                currHostWithSchema = utils.getHostWithSchemaOfLink(completeLink)
                hostInfo = self._hostsInfo.getHostInfo(currHostWithSchema)

                self._requestForRobotsOfHostIfNecessary(hostInfo)
                if self._shouldAccessPage(completeLink, hostInfo):

                    self._accessPageAndGetLinks(htmlParser, webAccess, completeLink, hostInfo)

                    if not hostInfo.emptyOfResources():
                        self._addHostToRequest(hostInfo.hostNameWithSchema, hostInfo.nextRequestAllowedTimestampFromNow())

                hostInfo.markResourceAsCrawled(utils.getResourcesFromLink(completeLink))
                shouldCheckForOtherLinksCount+=1

                if shouldCheckForOtherLinksCount == CHECK_FOR_OTHER_LINKS_EVERY_NUM_REQUESTS:
                    self._tryToCompleteWithReceivedLinks()
                    shouldCheckForOtherLinksCount = 0
            
            #wait
            self._workersPipeline.waitForLinkEvent(self._id)
            self._workersPipeline.unsetWorkerWaiting(self.id)

            if self._workersPipeline.allDone:
                logging.info("Terminou Operações")
                allWorkersFinished = True
            else:
                self._tryToCompleteWithReceivedLinks()
    
    def _shouldAccessPage(self, completeLink:str, hostInfo:Host.HostInfo) -> bool:

        allowed = hostInfo.canAccessPage(completeLink)

        passHeuristics = UnwantedPagesHeuristics.passHeuristicsAccess(completeLink)

        return allowed and passHeuristics

    def _accessPageAndGetLinks(self, htmlParser:Parser.HTMLParser, webAccess:WebAccesser, completeLink:str, hostInfo:Host.HostInfo):
        currHostWithSchema = hostInfo.hostNameWithSchema

        try:
            webAccess.GETRequest(completeLink)
        except (TimeoutError, NewConnectionError) as e:
            logging.exception(f"Erro de conexão com {completeLink}. Recolocando na fila para tentar de novo")
            
            #Add the same link again for retry later
            #as might have had internet problems
            self.addLinkToRequest(completeLink)
            
        except MaxRetryError as e:
            logging.exception(f"Max Retries reached ERROR for: {completeLink}")

        except Exception as e:
            logging.exception(f"Some error occurred whe requesting {completeLink}")

        else:
            logging.info(f"Fez requisição para: {completeLink}")

            if webAccess.lastRequestSuccess() and webAccess.lastResponseHasTextHtmlContent():
                
                treatedUrls = self._getAllLinksFromPage(htmlParser, webAccess, currHostWithSchema)

                self._distributeUrlsToWorkers(treatedUrls)

    def _getAllLinksFromPage(self, htmlPageParser:Parser.HTMLParser, webAccess:WebAccesser, currHostWithSchema:str):
        httpResponse = webAccess.lastResponseText()
        htmlPageParser.parse(httpResponse.data)
        urlsFound = htmlPageParser.getAllLinksFromParsedHTML()
        treatedUrls = htmlPageParser.formatUrlsWithHostIfNeeded(urlsFound, currHostWithSchema)
        return treatedUrls

    def _distributeUrlsToWorkers(self, treatedUrls):
        linksByWorker = self._workersPipeline.separateLinksByWorker(treatedUrls)
                            
        myLinks = linksByWorker[self._id]
        self.addAllLinksToRequest(myLinks)

        linksByWorker.pop(self._id, None)
        self._workersPipeline.sendLinksToProperWorkers(linksByWorker)

    def _getNextLinkToRequest(self):
        nextHost = self._getNextHostToRequest()
        nextHostResource = self._getNextResourceToRequestOfHost(nextHost)
        completeLink = self._getLinkFrom(nextHost, nextHostResource)
        return completeLink

    def _requestForRobotsOfHostIfNecessary(self, hostInfo:Host.HostInfo):
        if not hostInfo.hasRobots():
            hostInfo.tryFirstAccessToRobots()
            #talvez desnecessário
            hostInfo.saveLinksFromSitemapIfPossible()
    
    def _tryToCompleteWithReceivedLinks(self):
        
        linksWorkersSentToMe = self._workersPipeline.getLinksSentToWorker(self._id)
            
        while len(linksWorkersSentToMe) > 0:
            newLink = linksWorkersSentToMe.popleft()
            self.addLinkToRequest(newLink)

    def _hasLinkToRequest(self) -> bool:
        return not self._hostsQueue.empty()
    
    def _getNextHostToRequest(self) -> str:
        return self._hostsQueue.get()
    
    def _getNextResourceToRequestOfHost(self, host:str) -> str:
        hostInfo = self._hostsInfo.getHostInfo(host)
        return hostInfo.getNextResource()
    
    def _getLinkFrom(self, nextHost:str, nextHostResource:str) -> str:
        link = f"{nextHost}/{nextHostResource}"
        return link
    
    def getCrawlingInfo(self) -> str:
        hostsOnQueue = [host for host in self._hostsOnQueue]
        requestsMade = self._hostsInfo.getCrawledResourcesPerHost()
        requestsToBeDone = str(self._hostsInfo)

        return f"Hosts on Queue:\n{hostsOnQueue}\nRequests to be Done:\n{requestsToBeDone}\nRequests Made:\n{requestsMade}"