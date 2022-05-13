from WarcFileSave import WarcSaver
from DebugPrinter import JsonPrinter
from threading import Lock, Event
from collections import deque
from bs4 import BeautifulSoup
from Parser import HTMLParser
import logging
import urllib3
import utils

class WorkersPipeline():
    """
    This represents the object that the workers use to communicate to eachother
    """

    MAX_RESULTS_PER_WARC_FILE = 1000

    def __init__(self, workers:dict, maxNumPagesCrawled:int, debug:bool=False):
        self._workers = workers
        self._numWorkers = len(list(workers.keys()))
        
        self._debugMode = debug

        self._numPagesCrawledLock = Lock()
        self._numPagesCrawled = 0
        self._maxNumPagesToCrawl = maxNumPagesCrawled
        
        self._workerCommLinksRecv = {}
        self._workersCommLocks = {}
        self._numWorkersWaiting = 0
        self._numWorkersWaitingLock = Lock()
        self._workerWaitingLinksEvents = {}
        self._workerWaitingLinksEventsLocks = {}

        #For debugging purposes
        self._workersThatGotOut = dict()
        self._workersThatGotOutLock = Lock()
        for workerId in list(self._workers.keys()):
            self._workerCommLinksRecv[workerId] = deque()
            self._workersCommLocks[workerId] = Lock()

            self._workerWaitingLinksEvents[workerId] = Event()
            self._workerWaitingLinksEventsLocks[workerId] = Lock()

            self._workersThatGotOut[workerId] = False
        
        self._allDone = False
        self._allDoneLock = Lock()

        self._warcSaver = WarcSaver()
        self._debugPrinter = JsonPrinter()

        self._resourcesPerHost = dict()
        self._resourcesPerHostLock = Lock()

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
        self._allDoneLock.acquire()
        allDone = self._allDone
        self._allDoneLock.release()
        return allDone
    
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

    def getLinksSentToWorker(self, workerId:int) -> deque:
        receivedLinksLock = self._workersCommLocks[workerId]
        receivedLinksLock.acquire()
        self._workerWaitingLinksEventsLocks[workerId].acquire()
        
        linksReceived = self._workerCommLinksRecv[workerId].copy()
        self._workerCommLinksRecv[workerId].clear()

        self._workerWaitingLinksEvents[workerId].clear()

        self._workerWaitingLinksEventsLocks[workerId].release()
        receivedLinksLock.release()

        return linksReceived
    
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
                workerLock = self._workersCommLocks[workerId]

                workerLock.acquire()
                for hostAndResources in mappedLinks:
                    workerLinkDeque = self._workerCommLinksRecv[workerId]
                    currHost = hostAndResources[0]
                    resourcesOfHost = hostAndResources[1]
                    for resource in resourcesOfHost:
                        
                        completeLink = utils.getCompleteLinkFromHostAndResource(currHost, resource)
                        workerLinkDeque.append(completeLink)                    
               
                workerLock.release()
                self._signalWorkerReceivedLinkEvent(workerId)

    def _signalWorkerReceivedLinkEvent(self, workerId:int):
        self._workerWaitingLinksEventsLocks[workerId].acquire()
        self._workerWaitingLinksEvents[workerId].set()
        self._workerWaitingLinksEventsLocks[workerId].release()

    def waitForLinkOrAllDoneEvent(self, workerId:int):
        self._setWorkerWaiting()

        if not self.allDone and not self._shouldStop():
            self._workerWaitingLinksEvents[workerId].wait()

        self._unsetWorkerWaiting()
    
    def _setWorkerWaiting(self):
        self._numWorkersWaitingLock.acquire()
        self._numWorkersWaiting += 1
        self._numWorkersWaitingLock.release()
    
    def _unsetWorkerWaiting(self):
        self._numWorkersWaitingLock.acquire()
        self._numWorkersWaiting -= 1
        self._numWorkersWaitingLock.release()

    def _shouldStop(self):

        self._numWorkersWaitingLock.acquire()
        everyWorkerWaiting = self._numWorkersWaiting == self._numWorkers
        self._numWorkersWaitingLock.release()

        self._numPagesCrawledLock.acquire()
        shouldStop = self._crawledPassMaxNumPages()
        self._numPagesCrawledLock.release()

        if not shouldStop:
            [lock.acquire() for _, lock in self._workerWaitingLinksEventsLocks.items()]
            aWorkerSentLinksToAnother = any([event.is_set() for _, event in self._workerWaitingLinksEvents.items()])

            shouldStop = everyWorkerWaiting and not aWorkerSentLinksToAnother
            [lock.release() for _, lock in self._workerWaitingLinksEventsLocks.items()]

        if shouldStop:
            logging.info("TIME TO STOP")
            self.setAllDone()
            [lock.acquire() for _, lock in self._workerWaitingLinksEventsLocks.items()]
            self._wakeEveryWorkerToDie()
            [lock.release() for _, lock in self._workerWaitingLinksEventsLocks.items()]
        
        return shouldStop

    def setAllDone(self):
        self._allDoneLock.acquire()
        self._allDone = True
        self._allDoneLock.release()
    
    def _wakeEveryWorkerToDie(self):
        return [event.set() for _, event in self._workerWaitingLinksEvents.items()]
    
    def separateLinksByWorker(self, urls:set) -> dict:
        linkByHost = dict()

        for workerId in range(self._numWorkers):
            linkByHost[workerId] = list()

        for url in urls:
            hostWithSchema = utils.getHostWithSchemaOfLink(url)
            workerId = utils.threadOfHost(self._numWorkers, hostWithSchema)

            linkByHost[workerId].append(url)
        
        return linkByHost
    
    def setSaiu(self, workerId:int):
        self._workersThatGotOutLock.acquire()
        self._workersThatGotOut[workerId] = True
        self._workersThatGotOutLock.release()
    
    def getNaoSairam(self):
        self._workersThatGotOutLock.acquire()
        sairamString = f"{[threadId for threadId, gotOut in self._workersThatGotOut.items() if not gotOut]}"
        self._workersThatGotOutLock.release()
        return sairamString
    
    def saveResponse(self, response: urllib3.response.HTTPResponse, link:str):

        if self._warcSaver.saveAndReturnIfSuccess(response, link):
            self._addPageCrawledAndSaved(link)
    
    def _addPageCrawledAndSaved(self, link:str):
        
        self._numPagesCrawledLock.acquire()
        self._numPagesCrawled += 1
        logging.info(f"NUM PAGES: {self._numPagesCrawled}")
        
        if self._crawledPassMaxNumPages():
            logging.info(f"ATINGIU MAX PAGES")
            self.setAllDone()
        
        self._numPagesCrawledLock.release()
    
    def _crawledPassMaxNumPages(self) -> bool:
        return self._numPagesCrawled > self._maxNumPagesToCrawl
    
    def printIfOnDebugMode(self,  link:str, reqTimestamp:float, parsedHTML:BeautifulSoup):
        if self._debugMode:
            NUM_WORDS_TO_PRINT = 20
            textToPrint = HTMLParser.getNFirstTextWords(parsedHTML, NUM_WORDS_TO_PRINT)
            title = parsedHTML.find('title').string
            self._debugPrinter.printJson(link, reqTimestamp, title, textToPrint)
    
    def addResourcesPerHost(self, hostAndNumResourcesMap:dict):
        self._resourcesPerHostLock.acquire()
        for host, numResources in hostAndNumResourcesMap.items():
            if host in list(self._resourcesPerHost.keys()):
                self._resourcesPerHost[host] += numResources
            else:
                self._resourcesPerHost[host] = numResources
        self._resourcesPerHostLock.release()
    
    def getTotalResourcesPerHost(self):
        return self._resourcesPerHost