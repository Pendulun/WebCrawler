from queue import PriorityQueue, Queue
import utils

class MaxPagesToCrawlReachedError(Exception):
    pass

class Crawler():
    def __init__(self, pagesCrawledLimit):
        self._pagesLimit = pagesCrawledLimit
        self._pagesQueue = Queue() #Pode ser que vire um PriorityQueue
        self._pagesCrawled = 0
    
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

    def startCrawlingFromSeedsFile(self, seedsFilePath):
        self.__addPageLinksToCrawlFromFile(seedsFilePath)
        print(f"Queue size: {self._pagesQueue.qsize()}")
        #CRAWL FROM HERE

    
    def __addPageLinksToCrawlFromFile(self, seedsFilePath):
        with open(seedsFilePath, 'r') as seedsFile:
            print("Abriu Arquivo")
            
            line = seedsFile.readline()
            reachedMaxPagesEnqueued = False

            while line and not reachedMaxPagesEnqueued:
                try:
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
    