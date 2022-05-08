from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from threading import Lock, Event
from collections import deque
from bs4 import BeautifulSoup
import logging
import urllib3
from Parser import HTMLParser
import utils
import json

class WorkersPipeline():
    """
    This represents the object that the workers use to communicate to eachother
    """

    MAX_RESULTS_PER_WARC_FILE = 1000

    def __init__(self, workers:dict, maxNumPagesCrawled:int, debug:bool=False):
        self._workers = workers
        self._numWorkers = len(list(workers.keys()))
        
        self._debugMode = debug
        self._printLock = Lock()

        self._pagesCrawledLock = Lock()
        self._numPagesCrawled = 0
        self._maxNumPagesToCrawl = maxNumPagesCrawled
        self._maxPagesCrawledEvent = Event()
        self._maxPagesCrawledEventLock = Lock()
        
        self._workerToWorkerLink = {}
        self._workerToWorkerLocks = {}

        self._numWorkersWaiting = 0
        self._numWorkersWaitingLock = Lock()

        self._workerWaitingLinksEvents = {}
        self._workerWaitingLinksEventsLocks = {}

        self._workersThatGotOut = dict()
        self._workersThatGotOutLock = Lock()
        for workerId in list(self._workers.keys()):
            self._workerToWorkerLink[workerId] = deque()
            self._workerToWorkerLocks[workerId] = Lock()

            self._workerWaitingLinksEvents[workerId] = Event()
            self._workerWaitingLinksEventsLocks[workerId] = Lock()

            self._workersThatGotOut[workerId] = False
        
        self._allDone = False
        self._allDoneLock = Lock()

        self._warcOutputFilePreName = "results"
        self._warcOutputFileExtensions = ".warc.gz"
        self._currWarcFileId = 0
        self._warcFileLock = Lock()

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
        receivedLinksLock = self._getWorkerToWorkerLockOfWorker(workerId)
        receivedLinksLock.acquire()
        self._workerWaitingLinksEventsLocks[workerId].acquire()
        
        linksReceived = self._workerToWorkerLink[workerId]
        self._workerWaitingLinksEvents[workerId].clear()

        self._workerWaitingLinksEventsLocks[workerId].release()
        receivedLinksLock.release()

        return linksReceived
    
    def _getWorkerToWorkerLockOfWorker(self, workerId:int) -> Lock:
        return self._workerToWorkerLocks[workerId]
    
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
               
                workerLock.release()
                self._signalWorkerReceivedLinkEvent(workerId)

    def _signalWorkerReceivedLinkEvent(self, workerId):
        self._workerWaitingLinksEventsLocks[workerId].acquire()
        self._workerWaitingLinksEvents[workerId].set()
        self._workerWaitingLinksEventsLocks[workerId].release()
        
    def _unsetWorkerWaiting(self, workerId:int):
        self._numWorkersWaitingLock.acquire()
        self._numWorkersWaiting -= 1
        self._numWorkersWaitingLock.release()

    def waitForLinkOrAllDoneEvent(self, workerId:int):
        self._setWorkerWaiting(workerId)
        
        #I think that I dont need to acquire a lock
        #to run this line
        self._workerWaitingLinksEvents[workerId].wait()
        self._unsetWorkerWaiting(workerId)
    
    def _setWorkerWaiting(self, workerId:int):
        
        self._numWorkersWaitingLock.acquire()
        self._workerWaitingLinksEventsLocks[workerId].acquire()
        #[lock.acquire() for _, lock in self._workerWaitingLinksEventsLocks.items()]
        #self._workerWaitingLinksEventsLocks.acquire()

        self._numWorkersWaiting += 1

        #everyWorkerWaiting = all([waiting for _, waiting in self._workerWaitingLinks.items()])
        everyWorkerWaiting = self._numWorkersWaiting == self._numWorkers
        aWorkerSentLinksToAnother = any([event.is_set() for _, event in self._workerWaitingLinksEvents.items()])

        timeToStop = self.maxNumPagesReached() or (everyWorkerWaiting and not aWorkerSentLinksToAnother)

        if timeToStop:
            logging.info("TIME TO STOP")
            self._allDone = True
            self._wakeEveryWorkerToDie()
        
        #[lock.release() for _, lock in self._workerWaitingLinksEventsLocks.items()]
        self._workerWaitingLinksEventsLocks[workerId].release()
        self._numWorkersWaitingLock.release()
    
    def maxNumPagesReached(self) -> bool:
        self._maxPagesCrawledEventLock.acquire()
        isSet = self._maxPagesCrawledEvent.is_set()
        self._maxPagesCrawledEventLock.release()
        return isSet
    
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
    
    def getSairam(self):
        self._workersThatGotOutLock.acquire()
        sairamString = f"{self._workersThatGotOut}"
        self._workersThatGotOutLock.release()
        return sairamString
    
    def saveResponse(self, response: urllib3.response.HTTPResponse, link:str):

        self._pagesCrawledLock.acquire()
        
        self._addNumPagesCrawled(1)
        logging.info(f"NUM PAGES: {self._numPagesCrawled}")
        warcFileOutputName = self._getCurrWarcOutputFileName()
        
        self._pagesCrawledLock.release()

        self._saveOnWarcFile(response, link, warcFileOutputName)
    
    def printIfOnDebugMode(self,  link:str, reqTimestamp:float, parsedHTML:BeautifulSoup):
        if self._debugMode:
            NUM_WORDS_TO_PRINT = 20
            textToPrint = HTMLParser.getNFirstTextWords(parsedHTML, NUM_WORDS_TO_PRINT)
            title = parsedHTML.find('title').string
            self._printJson(link, reqTimestamp, title, textToPrint)

    def _saveOnWarcFile(self, response: urllib3.response.HTTPResponse, link:str, warcFileOutputName:str):
        self._warcFileLock.acquire()
        with open(warcFileOutputName, 'ab') as output:
            writer = WARCWriter(output, gzip=True)

            headers_list = response.getheaders().items()

            http_headers = StatusAndHeaders(str(response.status), headers_list, protocol='HTTP/1.0')

            record = writer.create_warc_record(link, 'response', payload=response,
                                                http_headers=http_headers)

            writer.write_record(record)
        
        self._warcFileLock.release()
    
    def _printJson(self, link:str, timestamp:float, title:str, text:str):
        jsonOut = {"URL": link,
                    "Title": title,
                    "Text": text,
                    "Timestamp":timestamp}
        
        self._printLock.acquire()
        #https://stackoverflow.com/a/18337754/16264901
        print(json.dumps(jsonOut, ensure_ascii=False).encode('utf-8').decode())
        self._printLock.release()
        
    def _getCurrWarcOutputFileName(self) -> str:
        return f"{self._warcOutputFilePreName}{self._currWarcFileId}{self._warcOutputFileExtensions}"

    def _addNumPagesCrawled(self, numPagesCrawled:int):
        
        self._numPagesCrawled += numPagesCrawled

        shouldSaveOnNewWarcFile = self._numPagesCrawled % WorkersPipeline.MAX_RESULTS_PER_WARC_FILE == 0
        if shouldSaveOnNewWarcFile :
            self._currWarcFileId +=1

        #Passou ou chegou no limite, define que é para parar
        if self._crawledMaxNumPages():
            logging.info(f"ATINGIU MAX PAGES")
            self._maxPagesCrawledEventLock.acquire()
            self._maxPagesCrawledEvent.set()
            self._maxPagesCrawledEventLock.release()
    
    def _crawledMaxNumPages(self) -> bool:
        return self._numPagesCrawled >= self._maxNumPagesToCrawl