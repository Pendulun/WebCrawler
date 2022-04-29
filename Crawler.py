from queue import PriorityQueue
from collections import deque
import utils
import urllib3
import certifi
from Worker import Worker
from WorkersPipeline import WorkersPipeline

class MaxPagesToCrawlReachedError(Exception):
    pass
    
class Crawler():
    def __init__(self, pagesCrawledLimit, numWorkers=1):
        
        self._workersQueues = {workerId:Worker(workerId) for workerId in range(numWorkers)}
        self._workersPipeline = WorkersPipeline(self._workersQueues)

        for (_, worker) in self._workersQueues.items():
            worker.workersPipeline = self._workersPipeline 

        self._pagesCrawled = 0
        self._pagesLimit = pagesCrawledLimit
        self._numWorkers = numWorkers
    
    @property
    def workersQueues(self):
        """The Workers"""
        raise AttributeError("workersQueues is not readable or writable")
    
    @workersQueues.setter
    def pagesLimit(self, newWorkersQueues):
        raise AttributeError("workersQueues is not readable or writable")
    
    @property
    def workersPipeline(self):
        """The communicator between workers"""
        raise AttributeError("workersPipeline is not readable or writable")
    
    @pagesLimit.setter
    def workersPipeline(self, newWorkersPipeline):
        raise AttributeError("workersPipeline is not readable or writable")
    
    @property
    def pagesLimit(self):
        """The maximum number of pages that can be crawled"""
        return self._pagesLimit
    
    @pagesLimit.setter
    def pagesLimit(self, newPagesLimit):
        self._pagesLimit = newPagesLimit
    
    @property
    def pagesCrawled(self):
        """The number of pages already crawled"""
        raise AttributeError("pagesQueue is not readable or writable")
    
    @pagesCrawled.setter
    def pagesCrawled(self, newValue):
        raise AttributeError("pagesQueue is not readable or writable")
    
    @property
    def numWorkers(self):
        """The number of workers that the Crawler may use"""
        return self._numWorkers
    
    @numWorkers.setter
    def numWorkers(self, newNumWorkers):
        raise AttributeError("newNumWorkers is read-only")

    def startCrawlingFromSeedsFile(self, seedsFilePath):
        self.__distributeSeedsForWorkers(seedsFilePath)
        #self.__crawUntilLimit()
        self.__getHostsAndResourcesFromWorkers()

    
    def __distributeSeedsForWorkers(self, seedsFilePath):
        with open(seedsFilePath, 'r') as seedsFile:
            print("Abriu Arquivo")
            
            link = seedsFile.readline().rstrip('\n')

            reachedMaxPagesEnqueued = False

            while link and not reachedMaxPagesEnqueued:
                try:
                    self.__sendLinkForThread(link)
                except MaxPagesToCrawlReachedError as e:
                    utils.printJoinedErrorMessage(e)
                    reachedMaxPagesEnqueued = True
                else:
                    link = seedsFile.readline().rstrip('\n')

    def __sendLinkForThread(self, newPageLink):
        
        if self._pagesCrawled < self._pagesLimit:
            
            threadOfHost = utils.threadOfHost(utils.getHostOfLink(newPageLink))
            
            self._workersQueues[threadOfHost].addLinkToRequest(newPageLink)

            #ISSO TA ERRADO
            self._pagesCrawled +=1
        else:
            raise MaxPagesToCrawlReachedError(  "Can't enqueue more pages to Crawl.",
                                                f" Limit of {self._pagesLimit} already reached!")
    
    def __crawUntilLimit(self):

        http = self._getCustomPoolManager()

        #Ver se vale a pena separar por host
        #https://urllib3.readthedocs.io/en/stable/advanced-usage.html#customizing-pool-behavior
        while not self.pagesQueue.empty():
            currPageLink = self.pagesQueue.get()
            
            httpResponse = http.request('GET', currPageLink)
            #httpResponse é do tipo urllib3.response.HTTPResponse
            #https://urllib3.readthedocs.io/en/stable/reference/urllib3.response.html?highlight=HTTPResponse#urllib3.response.HTTPResponse


            print(f"Fez requisição para: {currPageLink}")


            #Talvez tratar quando a resposta for redirecionada
            print(f"Recebeu resposta de: {httpResponse.geturl()}")

            print(f"Response status: {httpResponse.status}")
            print(httpResponse.data)

            if httpResponse.status == 200:
                print("Resposta 200")
    
    def __getHostsAndResourcesFromWorkers(self):
        workers = [worker for (_,worker) in self._workersQueues.items()]
        for workerId, worker in enumerate(workers):
            print(f"-----Worker:{workerId}-----")
            print(worker.getCrawlingInfo())

    def _getCustomPoolManager(self):
        customRetries = urllib3.Retry(3, redirect=10)
        return urllib3.PoolManager(
                                    retries=customRetries,
                                    cert_reqs='CERT_REQUIRED',
                                    ca_certs=certifi.where()
                                )
    
