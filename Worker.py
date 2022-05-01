from queue import PriorityQueue
from collections import deque
import utils
from WorkersPipeline import WorkersPipeline
import urllib3
import certifi
from threading import Lock, Condition
import logging
from reppy import Robots
from bs4 import BeautifulSoup
import requests

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
        self._hostsAndSchemaToResourses = dict()

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
        hostWithSchema, resources = utils.getHostWithSchemaAndResourcesFromLink(newLink)

        self._createHostResourcesQueueIfNotExists(hostWithSchema)
        
        if not self._alreadyCrawled(hostWithSchema, resources):
            self._putResourceIntoResourcesQueueOfHost(hostWithSchema, resources)
            self._addHostWithMaxPriorityToRequest(hostWithSchema)
    
    def _createHostResourcesQueueIfNotExists(self, host):
        if host not in list(self._hostsAndSchemaToResourses.keys()):
            self._hostsAndSchemaToResourses[host] = deque()
    
    def _alreadyCrawled(self, host, resource) -> bool:
        if host in list(self._crawledLinksPerHost.keys()):
            if resource in self._crawledLinksPerHost[host]:
                return True
            else:
                return False
        else:
            return False
    
    def _putResourceIntoResourcesQueueOfHost(self, host, resource):
        self._hostsAndSchemaToResourses[host].append(resource)
    
    def _addHostWithMaxPriorityToRequest(self, host):
        self._addHostToRequest(host, Worker.MAXHOSTPRIORITY)
    
    def _addHostToRequest(self, host, priority):
        if host not in self._hostsOnQueue:
            self._hostsOnQueue.add(host)
            self._hostsQueue.put((priority, host))
    
    def getCrawlingInfo(self) -> str:
        hostsOnQueue = [host for host in self._hostsOnQueue]
        requestsMade = self._crawledLinksPerHost
        requestsToBeDone = self._hostsAndSchemaToResourses

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
        while not finishedOperations:
            while self._hasLinkToRequest():
                
                nextHostWithSchema = self._getNextHostToRequest()
                nextHostResource = self._getNextResourceToRequestOfHost(nextHostWithSchema)
                completeLink = self._getLinkFrom(nextHostWithSchema, nextHostResource)

                if not self._haveCachedRobotsForHost(nextHostWithSchema):
                    self._findAndSaveRobotsOfHost(nextHostWithSchema)
                    self._saveLinksFromSitemapOfRobots(self._hostsPolicy[nextHostWithSchema])
                
                hostRobots = self._hostsPolicy[nextHostWithSchema]
                
                AGENTNAME = 'my-user-agent'
                if hostRobots.allowed(completeLink, AGENTNAME):
                    #Request
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

    def _hasLinkToRequest(self) -> bool:
        return not self._hostsQueue.empty()
    
    def _getNextHostToRequest(self) -> str:
        return self._hostsQueue.get()
    
    def _getNextResourceToRequestOfHost(self, host:str) -> str:
        return self._hostsAndSchemaToResourses[host].popleft()
    
    def _getLinkFrom(self, nextHost:str, nextHostResource:str) -> str:
        link = f"{nextHost}/{nextHostResource}"
        return link

    def _haveCachedRobotsForHost(self, host):
        return host in list(self._hostsPolicy.keys())
    
    def _findAndSaveRobotsOfHost(self, hostWithSchema):
        hostRobotsPath = Robots.robots_url(hostWithSchema)
        try:
            hostRobots = Robots.fetch(hostRobotsPath)
        except:
            self._hostsPolicy[hostWithSchema] = None
        else:
            self._hostsPolicy[hostWithSchema] = hostRobots
    
    def _saveLinksFromSitemapOfRobots(self, hostRobots:Robots):

        if hostRobots is not None:
            sitemapListOnRobots = hostRobots.sitemaps
            if len(sitemapListOnRobots) > 0:
                
                firstSitemap = sitemapListOnRobots[0]
                allLinksFound = self._findMaxLinksPossible(firstSitemap)

                self.addAllLinksToRequest(allLinksFound)

    def addAllLinksToRequest(self, links):
        for link in links:
            self.addLinkToRequest(link)

    def _findMaxLinksPossible(self, sitemapLink:str) -> list:
        sitemapPage = requests.get(sitemapLink)
        sitemapXml = sitemapPage.text
        sitemapSoup = BeautifulSoup(sitemapXml)

        pagesInThisSitemap = self._findPagesOnSitemapSoup(sitemapSoup)
        pagesInInnerSitemaps = self._findLinksFromInnerSitemaps(sitemapSoup)

        allLinksFound = list() 
        allLinksFound.extend(pagesInThisSitemap)
        allLinksFound.extend(pagesInInnerSitemaps)

        return allLinksFound
            
    def _findLinksFromInnerSitemaps(self, firstSitemapSoup) -> list:

        linkToPagesFound = list()
        sitemapTags = firstSitemapSoup.find_all("sitemap") 
        if len(sitemapTags) > 0:
            
            otherSitemaps = list()    
            for innerSitemap in sitemapTags:
                otherSitemaps.append(innerSitemap.findNext("loc").text)

            MAXNUMSITEMAPSCRAWLABLE = 5
            if len(otherSitemaps) > MAXNUMSITEMAPSCRAWLABLE:
                otherSitemaps = otherSitemaps[:MAXNUMSITEMAPSCRAWLABLE]
                     
            linkToPagesFound.extend(self._findLinksOnSitemaps(otherSitemaps))
        
        return linkToPagesFound

    def _findLinksOnSitemaps(self, sitemapsList:list) -> list:
        linksFound = list()

        for sitemapLink in sitemapsList:
            sitemapPage = requests.get(sitemapLink)
            sitemapXML = sitemapPage.text

            sitemapSoup = BeautifulSoup(sitemapXML)
            linksFound = self._findPagesOnSitemapSoup(sitemapSoup)
            linksFound.extend(linksFound)
        
        return linksFound
    
    def _findPagesOnSitemapSoup(self, sitemapSoup):
        linksToPagesFound = list()
        pageLinksFound = sitemapSoup.find_all("url")

        for page in pageLinksFound:
            pageLink = page.findNext("loc").text
            linksToPagesFound.append(pageLink)
        
        return linksToPagesFound

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
    
    # def _getCustomPoolManager(self):
    #     customRetries = urllib3.Retry(3, redirect=10)
    #     return urllib3.PoolManager(
    #                                 retries=customRetries,
    #                                 cert_reqs='CERT_REQUIRED',
    #                                 ca_certs=certifi.where()
    #                             )