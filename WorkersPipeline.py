from collections import deque
import utils
from threading import Lock

class WorkersPipeline():
    def __init__(self, workers:dict):
        self._workers = workers
        self._numWorkers = len(list(workers.keys()))
        
        self._workerToWorkerLink = {}
        
        for workerId in list(self._workers.keys()):
            self._workerToWorkerLink[workerId] = deque()
        
        self._workerToWorkerLock = {}
        for workerId in list(self._workers.keys()):
            self._workerToWorkerLock[workerId] = Lock()

    @property
    def numWorkers(self):
        return self._numWorkers
    
    @numWorkers.setter
    def numWorkers(self, newNumWorkers):
        raise AttributeError("newNumWorkers is not writable")

    @property
    def workers(self):
        return self._workers
    
    @workers.setter
    def workers(self, newWorkers):
        raise AttributeError("workers is not writable")
    
    def addLinksToProperWorkers(self, linkList:list):
        
        hostsToLinksMap = self._mapLinkResoursesToHosts(linkList)
        
        hostsAndResourcesToWorkerMap = self._mapResoursesToWorkers(hostsToLinksMap)

        self._sendResourcesToWorkers(hostsAndResourcesToWorkerMap)

    def _mapLinkResoursesToHosts(self, linkList):
        hostsToLinksMap = dict()
        for link in linkList:
            host, resource = utils.getHostAndResourcesFromLink(link)
            
            if host not in list(hostsToLinksMap.keys()):
                hostsToLinksMap[host] = []
            
            hostsToLinksMap[host] = resource
        return hostsToLinksMap
    
    def _mapResoursesToWorkers(self, hostsToLinksMap):
        hostsAndResourcesToWorkerMap = dict()
        for host, resources in hostsToLinksMap.items():
            workerId = utils.threadOfHost(self._numWorkers, host)

            if workerId not in list(hostsAndResourcesToWorkerMap.keys()):
                hostsAndResourcesToWorkerMap[workerId] = []
            
            hostsAndResourcesToWorkerMap[workerId].append((host, resources))
        return hostsAndResourcesToWorkerMap
    
    def _sendResourcesToWorkers(self, hostsAndResourcesToWorkerMap):
        for workerId, mappedLinks in hostsAndResourcesToWorkerMap.items():
            workerLock = self._workerToWorkerLock[workerId]

            workerLock.acquire()
            workerLinkDeque = self._workerToWorkerLink[workerId]
            workerLinkDeque.append(mappedLinks)
            workerLock.release()