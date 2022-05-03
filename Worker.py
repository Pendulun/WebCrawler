from threading import Lock, Condition
from WorkersPipeline import WorkersPipeline
from queue import PriorityQueue
from bs4 import BeautifulSoup
from collections import deque
from reppy import Robots
import utils
import urllib3
import certifi
import logging
import requests
import Host

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
        #REFATORAR PARA SUA PRÓPRIA CLASSE
        self._hostsInfo = Host.HostsInfo()

        #Set of crawled links per host
        self._crawledLinksPerHost = dict()

        self._totalPagesCrawled = 0
    
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
    def workersPipeline(self, newWorkersPipeline):
        self._workersPipeline = newWorkersPipeline

    @property
    def hostsQueue(self):
        raise AttributeError("hostsQueue is not readable")
    
    @hostsQueue.setter
    def hostsQueue(self, newHostsQueue):
        raise AttributeError("hostsQueue is not writable")
    
    @property
    def hostsResourses(self):
        raise AttributeError("hostsResourses is not readable")
    
    @hostsResourses.setter
    def hostsResourses(self, newHostsResourses):
        raise AttributeError("hostsResourses is not writable")
    
    @property
    def crawledLinksPerHost(self):
        raise AttributeError("crawledLinksPerHost is not readable")
    
    @crawledLinksPerHost.setter
    def crawledLinksPerHost(self, newCrawledLinksPerHost):
        raise AttributeError("crawledLinksPerHost is not writable")
    
    @property
    def totalPagesCrawled(self) -> int:
        return self._totalPagesCrawled
    
    @totalPagesCrawled.setter
    def totalPagesCrawled(self, newTotalPagesCrawled):
        raise AttributeError("totalPagesCrawled is not writable")
    
    def addAllLinksToRequest(self, links):
        for link in links:
            self.addLinkToRequest(link)

    def addLinkToRequest(self, newLink):
        hostWithSchema, resources = utils.getHostWithSchemaAndResourcesFromLink(newLink)

        if not self._alreadyCrawled(hostWithSchema, resources):
            self._hostsInfo.createInfoForHostIfNotExists(hostWithSchema)
            self._putResourceIntoResourcesQueueOfHost(hostWithSchema, resources)

            if hostWithSchema not in self._hostsOnQueue:
                self._addHostWithMaxPriorityToRequest(hostWithSchema)
    
    def _alreadyCrawled(self, host, resource) -> bool:
        if host in list(self._crawledLinksPerHost.keys()):
            if resource in self._crawledLinksPerHost[host]:
                return True
            else:
                return False
        else:
            return False
    
    def _putResourceIntoResourcesQueueOfHost(self, host:str, resource:str):
        hostInfo = self._hostsInfo.getHostInfo(host)
        hostInfo.addResource(resource)
    
    def _addHostWithMaxPriorityToRequest(self, host:str):
        self._addHostToRequest(host, Worker.MAXPRIORITYFORHOST)
    
    def _addHostToRequest(self, host:str, priority:int):
        if host not in self._hostsOnQueue:
            self._hostsOnQueue.add(host)
            self._hostsQueue.put((priority, host))
    
    def getCrawlingInfo(self) -> str:
        hostsOnQueue = [host for host in self._hostsOnQueue]
        requestsMade = self._crawledLinksPerHost
        requestsToBeDone = str(self._hostsInfo)

        return f"Hosts on Queue:\n{hostsOnQueue}\nRequests to be Done:\n{requestsToBeDone}\nRequests Made:\n{requestsMade}"

    def crawl(self):
        # http = self._getCustomPoolManager()

        # #Ver se vale a pena separar por host
        # #https://urllib3.readthedocs.io/en/stable/advanced-usage.html#customizing-pool-behavior
        # while not self.pagesQueue.empty():
        #     currPageLink = self.pagesQueue.get()
            
        #     httpResponse = http.request('GET', currPageLink)
        #     #httpResponse é do tipo urllib3.response.HTTPResponse
        #     #https://urllib3.readthedocs.io/en/stable/reference/urllib3.response.html?highlight=HTTPResponse#urllib3.response.HTTPResponse


        #     print(f"Fez requisição para: {currPageLink}")


        #     #Talvez tratar quando a resposta for redirecionada
        #     print(f"Recebeu resposta de: {httpResponse.geturl()}")

        #     print(f"Response status: {httpResponse.status}")
        #     print(httpResponse.data)

        #     if httpResponse.status == 200:
        #         print("Resposta 200")
        logging.info(f"Hello from Thread {self._id}")

        finishedOperations = False
        webAccess = self._getCustomPoolManager()
        while not finishedOperations:
            while self._hasLinkToRequest():
                
                currHostWithSchema = self._getNextHostToRequest()
                currHostResource = self._getNextResourceToRequestOfHost(currHostWithSchema)
                completeLink = self._getLinkFrom(currHostWithSchema, currHostResource)
                
                hostInfo = self._hostsInfo.getHostInfo(currHostWithSchema)

                self._requestForRobotsOfHostIfNecessary(hostInfo)
                
                if self._shouldAccessPage(completeLink, hostInfo):

                    httpResponse = webAccess.request('GET', completeLink)
                    logging.info(f"Fez requisição para: {completeLink}")

                    if self._responseSuccess(httpResponse):

                        pageText = BeautifulSoup(httpResponse.text)
                        urlsFound = self._getAllLinksInsideHtml(pageText)
                        treatedUrls = self._formatUrls(urlsFound, currHostWithSchema)

                        linksByWorker = self._separateLinksByWorker(treatedUrls)
                        
                        myLinks = linksByWorker[self._id]
                        self.addAllLinksToRequest(myLinks)

                        linksByWorker.pop(self._id, None)
                        self._sendLinksToProperWorkers(linksByWorker)

                        #If host resources is not empty, add it again to the priorityQueue
                        #with proper timestamp

                        #Mark this page as already crawled
                        pass
                    
                
            self._tryToCompleteWithReceivedLinks()

            #barreira
            if not self._hasLinkToRequest():
                #Espera alguém avisar que pode sair
                #Avisar o Pipeline que estou esperando
                pass

            if not self._hasLinkToRequest():
                logging.info("Terminou Operações")
                finishedOperations = True
    
    def _sendLinksToProperWorkers(self, linksByWorker:dict):
        for workerId, linksToSend in linksByWorker.items():
            logging.info(f"Thread {self._id} enviando para Thread {workerId}")
            self._workersPipeline.sendLinksToWorker(linksToSend, workerId)

    def _requestForRobotsOfHostIfNecessary(self, hostInfo:Host.HostInfo):
        if not hostInfo.hasRobots():
            hostInfo.tryFirstAccessToRobots()
            hostInfo.saveLinksFromSitemapIfPossible()
    
    def _separateLinksByWorker(self, urls:set) -> dict:
        linkByHost = dict()

        for workerId in range(self._workersPipeline.numWorkers):
            linkByHost[workerId] = list()

        for url in urls:
            hostWithSchema = utils.getHostWithSchemaOfLink(url)
            workerId = utils.threadOfHost(self._workersPipeline.numWorkers, hostWithSchema)

            linkByHost[workerId] = url
        
        return linkByHost

    
    def _formatUrls(self, urlsFound:set, currHostWithSchema:str) -> set:
        formatedUrls = set()

        for url in urlsFound:
            if url[0] != "#":
                formatedUrl = ""

                if url[0] == "/":
                    formatedUrl = f"{currHostWithSchema}{url}"
                elif (len(url) >= 4 and url[:4] == "http") or (len(url) >= 5 and url[:5] == "https"):
                    formatedUrl = url

                if(formatedUrl != ""):
                    formatedUrls.add(formatedUrl)
        
        return formatedUrls

    def _getAllLinksInsideHtml(self, pageText:BeautifulSoup) -> set:
        #Find all links
        allAnchorsFound = pageText.find_all("a")

        urlsFound = set()
        for anchorTag in allAnchorsFound:
            urlsFound.add(anchorTag.get("href"))
        
        return urlsFound

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

    def _tryToCompleteWithReceivedLinks(self):
        workerToWorkerLock = self._workersPipeline.getWorkerToWorkerLockOfWorker(self._id)
        workerToWorkerLock.acquire()

        linksWorkersSentToMe = self._workersPipeline.getWorkerToWorkerOfWorker(self._id)
        if(len(linksWorkersSentToMe) > 0):
            #Completar a minha Queue
            while linksWorkersSentToMe:
                newLink = linksWorkersSentToMe.popleft()
                self.addLinkToRequest(newLink)

        workerToWorkerLock.release()
    
    def _getCustomPoolManager(self):
        customRetries = urllib3.Retry(3, redirect=10)
        return urllib3.PoolManager(
                                    retries=customRetries,
                                    cert_reqs='CERT_REQUIRED',
                                    ca_certs=certifi.where()
                                )
    
    def _shouldAccessPage(self, completeLink:str, hostInfo:Host.HostInfo) -> bool:

        allowed = hostInfo.canAccessPage(completeLink)

        passHeuristics = UnwantedPagesHeuristics.passHeuristicsAccess(completeLink)

        return allowed and passHeuristics
    
    def _responseSuccess(self, httpResponse):
        return str(httpResponse.status)[0] == "2"