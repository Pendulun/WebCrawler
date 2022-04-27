from queue import PriorityQueue, Queue
import utils
import urllib3
import certifi

class MaxPagesToCrawlReachedError(Exception):
    pass

class HostsInfoContainer():
    def __init__(self):
        self._hostsInfoMap = dict()
    
    @property
    def hostsInfoMap(self):
        return self._hostsInfoMap
    
    @hostsInfoMap.setter
    def hostsInfoMap(self, newHostsInfoMap):
        raise AttributeError("hostsInfoMap is read only!")

    def addHost(self, hostLink):
        pass

class HostInfo():
    def __init__(self, hostName):
        self._hostName = hostName
    
    @property
    def hostName(self):
        return self._hostName
    
    @hostName.setter
    def hostName(self, newHostName):
        raise AttributeError("hostName is read only!")


class Crawler():
    def __init__(self, pagesCrawledLimit, numWorkers=1):
        self._pagesLimit = pagesCrawledLimit
        self._pagesQueue = Queue() #Pode ser que vire um PriorityQueue
        self._pagesCrawled = 0
        self._numWorkers = numWorkers
        # self._hostsInfo = TERMINAR DE CRIAR UM MAP DE HOSTS   
    
    @property
    def pagesQueue(self):
        """The Queue of pages to be crawled"""
        return self._pagesQueue

    @pagesQueue.setter
    def pagesQueue(self, newQueue):
        raise AttributeError("pagesQueue is read only")
    
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
        self._numWorkers = newNumWorkers

    def startCrawlingFromSeedsFile(self, seedsFilePath):
        self.__addPageLinksToCrawlFromFile(seedsFilePath)
        print(f"Queue size: {self._pagesQueue.qsize()}")
        self.__crawUntilLimit()

    
    def __addPageLinksToCrawlFromFile(self, seedsFilePath):
        with open(seedsFilePath, 'r') as seedsFile:
            print("Abriu Arquivo")
            
            line = seedsFile.readline()
            reachedMaxPagesEnqueued = False

            while line and not reachedMaxPagesEnqueued:
                try:
                    #LER O ROBOTS DO HOST
                    #https://docs.python.org/3/library/urllib.robotparser.html
                    #https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html?highlight=parse#urllib3.util.parse_url
                    #https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html?highlight=parse#urllib3.util.Url
                    self.__addPageLinkToCrawl(line)
                except MaxPagesToCrawlReachedError as e:
                    utils.printJoinedErrorMessage(e)
                    reachedMaxPagesEnqueued = True
                else:
                    line = seedsFile.readline()

    def __addPageLinkToCrawl(self, newPageLink):
        
        if self._pagesCrawled < self._pagesLimit:

            self._pagesQueue.put(newPageLink)
            self._pagesCrawled +=1
            print(f"Adicionou link: {newPageLink}")

        else:
            raise MaxPagesToCrawlReachedError(   "Can't enqueue more pages to Crawl.",
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



        #Enquanto a fila não estiver vazia
        # Retirar uma página da fila

        # Realizar a requisição

        # Baixar a página

        # Extrair links

        # Extrair info
        #https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.parse_url
        #https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Url

        # Colocar links extraídos na fila se der
    
    def _getCustomPoolManager(self):
        customRetries = urllib3.Retry(3, redirect=10)
        return urllib3.PoolManager(
                                    retries=customRetries,
                                    cert_reqs='CERT_REQUIRED',
                                    ca_certs=certifi.where()
                                )
    
