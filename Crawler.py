from Worker import Worker
from WorkersPipeline import WorkersPipeline
import threading
import logging
import utils
    
class Crawler():
    """
    This is the crawler. It manages all workers to crawl pages until the limit is reached
    """
    def __init__(self, pagesCrawledLimit:int, numWorkers:int = 1, debugMode:bool = False):
        
        self._workersQueues = {workerId:Worker(workerId) for workerId in range(1, numWorkers+1)}
        self._workersPipeline = WorkersPipeline(self._workersQueues, pagesCrawledLimit, debugMode)

        for (_, worker) in self._workersQueues.items():
            worker.workersPipeline = self._workersPipeline 

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
    def pagesCrawled(self) -> int:
        """The number of pages already crawled"""
        return self._workersPipeline.pagesCrawled
    
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

    def startCrawlingFromSeedsFile(self, seedsFilePath: str):
        self.__distributeSeedsForWorkers(seedsFilePath)
        self.__crawlWorkers()
    
    def __distributeSeedsForWorkers(self, seedsFilePath: str):
        
        with open(seedsFilePath, 'r') as seedsFile:
            logging.info("Abriu Arquivo")
            
            link = seedsFile.readline().rstrip('\n')

            while link:
                self.__sendLinkForThread(link)
                link = seedsFile.readline().rstrip('\n')

    def __sendLinkForThread(self, newPageLink: str):
        
        threadOfHost = utils.threadOfHost(self._numWorkers, utils.getHostWithSchemaOfLink(newPageLink))
        self._workersQueues[threadOfHost].addLinkToRequest(newPageLink)

    def __crawlWorkers(self):
        
        workersThreads = list()

        for _, worker in self._workersQueues.items():
            newThread = threading.Thread(target=worker.crawl)
            workersThreads.append(newThread)
            newThread.start()

        [thread.join() for thread in workersThreads]

        #Pegar estatísticas
    
    def __getHostsAndResourcesFromWorkers(self):
        workers = [worker for (_,worker) in self._workersQueues.items()]
        for workerId, worker in enumerate(workers):
            logging.info(f"\n-----Worker:{workerId}-----\n{worker.getCrawlingInfo()}")