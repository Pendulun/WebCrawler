from collections import deque
import utils
from threading import Lock, Condition
import logging

class WorkersPipeline():
    """
    This represents the object that the workers use to communicate to eachother
    """

    def __init__(self, workers:dict):
        self._workers = workers
        self._numWorkers = len(list(workers.keys()))
        
        self._workerToWorkerLink = {}
        
        for workerId in list(self._workers.keys()):
            self._workerToWorkerLink[workerId] = deque()
        
        self._workerToWorkerLock = {}
        for workerId in list(self._workers.keys()):
            self._workerToWorkerLock[workerId] = Lock()
        
        # https://docs.python.org/3/library/threading.html#condition-objects
        self._workerWaitingLinks = {}
        self._workerWaitingLinksCondVar = {}
        self._workerWaitingLinksLock = {}
        for workerId in list(self._workers.keys()):
            self._workerWaitingLinks[workerId] = False
            self._workerWaitingLinksLock[workerId] = Lock()
            self._workerWaitingLinksCondVar[workerId] = Condition(self._workerWaitingLinksLock[workerId])
            
    @property
    def numWorkers(self) -> int:
        return self._numWorkers
    
    @numWorkers.setter
    def numWorkers(self, newNumWorkers):
        raise AttributeError("newNumWorkers is not writable")

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

    def getWorkerToWorkerLockOfWorker(self, workerId:int) -> Lock:
        return self._workerToWorkerLock[workerId]
    
    def getWorkerToWorkerOfWorker(self, workerId:int) -> deque:
        return self._workerToWorkerLink[workerId]
    
    def addLinksToProperWorkers(self, linkList:list):
        
        hostsWithSchemaToLinksMap = self._mapLinkResoursesToHosts(linkList)
        
        hostsAndResourcesToWorkerMap = self._mapResoursesToWorkers(hostsWithSchemaToLinksMap)

        self._sendResourcesToWorkers(hostsAndResourcesToWorkerMap)

    def _mapLinkResoursesToHosts(self, linkList:list) -> dict:
        hostsToLinksMap = dict()
        for link in linkList:
            hostWithSchema, resource = utils.getHostWithSchemaAndResourcesFromLink(link)
            
            if hostWithSchema not in list(hostsToLinksMap.keys()):
                hostsToLinksMap[hostWithSchema] = set()
            
            hostsToLinksMap[hostWithSchema] = resource
        return hostsToLinksMap
    
    def _mapResoursesToWorkers(self, hostsWithSchemaToLinksMap: dict) -> dict:
        hostsAndResourcesToWorkerMap = dict()

        for workerId in range(self._numWorkers):
            hostsAndResourcesToWorkerMap[workerId] = list()
        
        for hostWithSchema, resources in hostsWithSchemaToLinksMap.items():
            workerId = utils.threadOfHost(self._numWorkers, hostWithSchema)
            
            hostsAndResourcesToWorkerMap = self._addHostAndResourcesToWorkerQueue(  hostsAndResourcesToWorkerMap,
                                                                                    workerId, hostWithSchema, 
                                                                                    resources
                                                                                    )

        return hostsAndResourcesToWorkerMap

    def _addHostAndResourcesToWorkerQueue(self, hostsAndResourcesToWorkerMap:dict, workerId:int, hostWithSchema:str, resources:list):
        hostsAndResourcesToWorkerMap[workerId].append((hostWithSchema, resources))
        return hostsAndResourcesToWorkerMap
    
    def _sendResourcesToWorkers(self, hostsAndResourcesToWorkerMap:dict):
        for workerId, mappedLinks in hostsAndResourcesToWorkerMap.items():
            if len(mappedLinks) > 0:
                workerLock = self._workerToWorkerLock[workerId]

                workerLock.acquire()
                logging.info(f"Worker Pipeline colocando elementos na fila do Worker {workerId}")
                workerLinkDeque = self._workerToWorkerLink[workerId]
                for link in mappedLinks:
                    workerLinkDeque.append(link)
                workerLock.release()
    
    def sendLinksToWorker(self, links:list, workerId:int):
        hostsWithSchemaToLinksMap = self._mapLinkResoursesToHosts(links)

        hostsAndResourcesToWorkerMap = dict()
        hostsAndResourcesToWorkerMap[workerId] = list()
        for hostWithSchema, resources in hostsWithSchemaToLinksMap.items():
            hostsAndResourcesToWorkerMap = self._addHostAndResourcesToWorkerQueue(  hostsAndResourcesToWorkerMap,
                                                                                    workerId, hostWithSchema,
                                                                                    resources
                                                                                    )

        self._sendResourcesToWorkers(hostsAndResourcesToWorkerMap)