from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter
from threading import Lock
import urllib3
import logging

class WarcSaver():
    
    MAX_RESULTS_PER_WARC_FILE = 1000

    def __init__(self, warcPreName = "results"):
        self._warcOutputFilePreName = warcPreName
        self._warcOutputFileExtensions = ".warc.gz"
        
        self._warcFileLock = Lock()
        
        self._numSavedPages = 0
        self._currWarcFileId = 0
    
    def save(self, response: urllib3.response.HTTPResponse, link:str):
        self._warcFileLock.acquire()
        success = False
        with open(self._getCompleteWarcOutputFileName(), 'ab') as output:
            try:
                writer = WARCWriter(output, gzip=True)

                headers_list = response.getheaders().items()

                http_headers = StatusAndHeaders(str(response.status), headers_list, protocol='HTTP/1.0')

            
                record = writer.create_warc_record(link, 'response', payload=response,
                                                    http_headers=http_headers)

                writer.write_record(record)

                self._numSavedPages +=1

                shouldSaveOnNewWarcFile = self._numSavedPages % WarcSaver.MAX_RESULTS_PER_WARC_FILE == 0
                if shouldSaveOnNewWarcFile :
                    self._currWarcFileId +=1
                    logging.info("Atualizou nome warc file")

                success = True

            except:
                success = False
        
        self._warcFileLock.release()

        return success
    
    def _getCompleteWarcOutputFileName(self):
        return f"{self._warcOutputFilePreName}{self._currWarcFileId}{self._warcOutputFileExtensions}"
    
    @property
    def numSavedPages(self):
        return self._numSavedPages
    
    @numSavedPages.setter
    def numSavedPages(self, newSavedPages:int):
        raise AttributeError("numSavedPages is not writable")

    @property
    def warcFileId(self):
        return self._currWarcFileId
    
    @warcFileId.setter
    def warcFileId(self, newWarcFileId:int):
        raise AttributeError("warcFileId is not writable")