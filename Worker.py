from queue import PriorityQueue
from collections import deque
import utils
from WorkersPipeline import WorkersPipeline
import urllib3
import certifi
from threading import Lock, Condition
import logging

class Worker():

    """
    This is a worker that effectively crawls web pages
    """

    MAXHOSTPRIORITY = 0
    
    def __init__(self, id):
        #Worker Id
        self._id = id

        #Communicator between workers
        self._workersPipeline = WorkersPipeline({})

        #Next host to request from
        self._hostsQueue = PriorityQueue()

        #Set of hosts currently on the _hostsQueue
        self._hostsOnQueue = set()

        #Links to request from a host
        self._hostsResourses = dict()

        #Robots policy for each host
        self._hostsPolicy = dict()

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
    def hostsPolicy(self):
        raise AttributeError("hostsPolicy is not readable")
    
    @hostsPolicy.setter
    def hostsPolicy(self, newHostsPolicy):
        raise AttributeError("hostsPolicy is not writable")
    
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
    
    def addLinkToRequest(self, newLink):
        hostFromLink, resourcesFromLink = utils.getHostAndResourcesFromLink(newLink)

        self._createHostResourcesQueueIfNotExists(hostFromLink)
        
        if not self._alreadyCrawled(hostFromLink, resourcesFromLink):
            self._putResourceIntoResourcesQueueOfHost(hostFromLink, resourcesFromLink)
            self._addHostWithMaxPriorityToRequest(hostFromLink)
    
    def _createHostResourcesQueueIfNotExists(self, host):
        if host not in list(self._hostsResourses.keys()):
            self._hostsResourses[host] = deque()
    
    def _alreadyCrawled(self, host, resource) -> bool:
        if host in list(self._crawledLinksPerHost.keys()):
            if resource in self._crawledLinksPerHost[host]:
                return True
            else:
                return False
        else:
            return False
    
    def _putResourceIntoResourcesQueueOfHost(self, host, resource):
        self._hostsResourses[host].append(resource)
    
    def _addHostWithMaxPriorityToRequest(self, host):
        self._addHostToRequest(host, Worker.MAXHOSTPRIORITY)
    
    def _addHostToRequest(self, host, priority):
        if host not in self._hostsOnQueue:
            self._hostsOnQueue.add(host)
            self._hostsQueue.put((priority, host))
    
    def getCrawlingInfo(self) -> str:
        hostsOnQueue = [host for host in self._hostsOnQueue]
        requestsMade = self._crawledLinksPerHost
        requestsToBeDone = self._hostsResourses

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
        logging.info(f"Olá da Thread {self._id}")

        # finishedOperations = False
        # while not finishedOperations:
        #     while self._hasLinkToRequest():
        #         #Request
        #         pass
                
        #     self._tryToCompleteWithReceivedLinks()

        #     #barreira
        #     if not self._hasLinkToRequest():
        #         #Espera alguém avisar que pode sair
        #         #Avisar o Pipeline que estou esperando
        #         pass

        #     if not self._hasLinkToRequest():
        #         finishedOperations = True

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

    
    def _hasLinkToRequest(self) -> bool:
        return not self._hostsQueue.empty()
        

    # def _getCustomPoolManager(self):
    #     customRetries = urllib3.Retry(3, redirect=10)
    #     return urllib3.PoolManager(
    #                                 retries=customRetries,
    #                                 cert_reqs='CERT_REQUIRED',
    #                                 ca_certs=certifi.where()
    #                             )