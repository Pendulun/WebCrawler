from collections import deque
import utils
from threading import Lock, Event
import logging

class WorkersPipeline():
    """
    This represents the object that the workers use to communicate to eachother
    """

    def __init__(self, workers:dict, maxNumPagesCrawled:int):
        self._workers = workers
        self._numWorkers = len(list(workers.keys()))

        self._pagesCrawledLock = Lock()
        self._numPagesCrawled = 0
        self._maxNumPagesToCrawl = maxNumPagesCrawled
        self._maxPagesCrawledEvent = Event()
        self._maxPagesCrawledEventLock = Lock()
        
        self._workerToWorkerLink = {}
        self._workerToWorkerLock = {}

        # https://docs.python.org/3/library/threading.html#condition-objects
        self._workerWaitingLinks = {}
        self._workerWaitingLinksDictLock = Lock()

        self._workerWaitingLinksEvent = {}
        self._workerWaitingLinksEventLock = Lock()
        for workerId in list(self._workers.keys()):
            self._workerToWorkerLink[workerId] = deque()
            self._workerToWorkerLock[workerId] = Lock()

            self._workerWaitingLinks[workerId] = False
            self._workerWaitingLinksEvent[workerId] = Event()
        
        self._allDone = False
            
    @property
    def numWorkers(self) -> int:
        return self._numWorkers
    
    @numWorkers.setter
    def numWorkers(self, newNumWorkers):
        raise AttributeError("newNumWorkers is not writable")
    
    @property
    def pagesCrawled(self) -> int:
        return self._numPagesCrawled
    
    @pagesCrawled.setter
    def pagesCrawled(self, pagesCrawled):
        raise AttributeError("pagesCrawled is not writable")
    
    @property
    def allDone(self) -> bool:
        return self._allDone
    
    @allDone.setter
    def allDone(self, newAllDone):
        raise AttributeError("allDone is not writable")

    @property
    def workers(self) -> dict:
        return self._workers
    
    @workers.setter
    def workers(self, newWorkers):
        raise AttributeError("workers is not writable")
    
    @property
    def workerToWorkerLink(self):
        raise AttributeError("workerToWorkerLink is not readable or writable")
    
    @workerToWorkerLink.setter
    def workerToWorkerLink(self, newWorkerToWorkerLink):
        raise AttributeError("workerToWorkerLink is not readable or writable")
    
    @property
    def workerToWorkerLock(self):
        raise AttributeError("workerToWorkerLock is not readable or writable")
    
    @workerToWorkerLock.setter
    def workerToWorkerLock(self, newWorkerToWorkerLock):
        raise AttributeError("workerToWorkerLock is not readable or writable")
    
    @property
    def workerWaitingLinks(self):
        raise AttributeError("workerWaitingLinks is not readable or writable")
    
    @workerWaitingLinks.setter
    def workerWaitingLinks(self, newWorkerWaitingLinks):
        raise AttributeError("workerWaitingLinks is not readable or writable")
    
    def addNumPagesCrawled(self, numPagesCrawled:int):
        self._pagesCrawledLock.acquire()
        self._numPagesCrawled += numPagesCrawled

        #Passou ou chegou no limite, define que é para parar
        if self._crawledMaxNumPages():
            logging.info(f"ATINGIU MAX PAGES")
            self._maxPagesCrawledEventLock.acquire()
            self._maxPagesCrawledEvent.set()
            self._maxPagesCrawledEventLock.release()

        self._pagesCrawledLock.release()
    
    def _crawledMaxNumPages(self) -> bool:
        return self._numPagesCrawled >= self._maxNumPagesToCrawl
    
    def maxNumPagesReached(self) -> bool:
        self._maxPagesCrawledEventLock.acquire()
        isSet = self._maxPagesCrawledEvent.is_set()
        self._maxPagesCrawledEventLock.release()
        return isSet

    def getLinksSentToWorker(self, workerId:int) -> deque:
        receivedLinksLock = self._getWorkerToWorkerLockOfWorker(workerId)
        receivedLinksLock.acquire()
        self._workerWaitingLinksEventLock.acquire()
        
        linksReceived = self._workerToWorkerLink[workerId]
        self._workerWaitingLinksEvent[workerId].clear()

        self._workerWaitingLinksEventLock.release()
        receivedLinksLock.release()

        return linksReceived
    
    def _getWorkerToWorkerLockOfWorker(self, workerId:int) -> Lock:
        return self._workerToWorkerLock[workerId]
    
    def sendLinksToProperWorkers(self, linksByWorker:dict):
        hostsAndResourcesToWorkerMap = dict()
        for workerId, linksToSend in linksByWorker.items():
            hostsWithSchemaToLinksMap = self._mapLinkResoursesToHosts(linksToSend)
            hostsAndResourcesToWorkerMap[workerId] = [
                    (hostWithSchema, resources) for hostWithSchema, resources in hostsWithSchemaToLinksMap.items()
                    ]
            
            self._sendResourcesToWorkers(hostsAndResourcesToWorkerMap)
    
    def _mapLinkResoursesToHosts(self, linkList:list) -> dict:
        hostsToLinksMap = dict()
        for link in linkList:
            hostWithSchema, resource = utils.getHostWithSchemaAndResourcesFromLink(link)
            
            if hostWithSchema not in list(hostsToLinksMap.keys()):
                hostsToLinksMap[hostWithSchema] = set()
            
            hostsToLinksMap[hostWithSchema].add(resource)
        return hostsToLinksMap
    
    def _sendResourcesToWorkers(self, hostsAndResourcesToWorkerMap:dict):
        for workerId, mappedLinks in hostsAndResourcesToWorkerMap.items():
            if len(mappedLinks) > 0:
                #mappedLinks is a list of tuples
                #each tuple has a host as a first value and a set of resources of that
                #host as a second value
                workerLock = self._getWorkerToWorkerLockOfWorker(workerId)

                workerLock.acquire()
                for hostAndResources in mappedLinks:
                    workerLinkDeque = self._workerToWorkerLink[workerId]
                    currHost = hostAndResources[0]
                    resourcesOfHost = hostAndResources[1]
                    for resource in resourcesOfHost:
                        
                        completeLink = utils.getCompleteLinkFromHostAndResource(currHost, resource)
                        workerLinkDeque.append(completeLink)                    
               
                self._signalWorkerReceivedLinkEvent(workerId)
                workerLock.release()

    def _signalWorkerReceivedLinkEvent(self, workerId):
        self._workerWaitingLinksEventLock.acquire()
        self._workerWaitingLinksEvent[workerId].set()
        self._workerWaitingLinksEventLock.release()
        
    def _unsetWorkerWaiting(self, workerId:int):
        self._workerWaitingLinksDictLock.acquire()
        self._workerWaitingLinks[workerId] = False
        self._workerWaitingLinksDictLock.release()

    def waitForLinkOrAllDoneEvent(self, workerId:int):
        self._setWorkerWaiting(workerId)
        
        #I think that I dont need to acquire a lock
        #to run this line
        self._workerWaitingLinksEvent[workerId].wait()
        logging.info("SAIU DO WAIT")
        self._unsetWorkerWaiting(workerId)
    
    def _setWorkerWaiting(self, workerId:int):
        
        self._workerWaitingLinksDictLock.acquire()
        self._workerWaitingLinksEventLock.acquire()

        self._workerWaitingLinks[workerId] = True

        everyWorkerWaiting = all([waiting for _, waiting in self._workerWaitingLinks.items()])
        aWorkerSentLinksToAnother = any([event.is_set() for _, event in self._workerWaitingLinksEvent.items()])

        if everyWorkerWaiting and not aWorkerSentLinksToAnother:
            logging.info("Percebeu que todos esperando e ninguém enviou mais")

        if self.maxNumPagesReached():
            logging.info("Percebeu que já atingiu o máximo de requests com sucesso!")

        timeToStop = self.maxNumPagesReached() or (everyWorkerWaiting and not aWorkerSentLinksToAnother)

        if timeToStop:
            logging.info("TIME TO STOP")
            self._allDone = True
            self._wakeEveryWorkerToDie()
        
        self._workerWaitingLinksEventLock.release()
        self._workerWaitingLinksDictLock.release()
    
    def _wakeEveryWorkerToDie(self):
        return [event.set() for _, event in self._workerWaitingLinksEvent.items()]
    
    def separateLinksByWorker(self, urls:set) -> dict:
        linkByHost = dict()

        for workerId in range(self._numWorkers):
            linkByHost[workerId] = list()

        for url in urls:
            hostWithSchema = utils.getHostWithSchemaOfLink(url)
            workerId = utils.threadOfHost(self._numWorkers, hostWithSchema)

            linkByHost[workerId].append(url)
        
        return linkByHost