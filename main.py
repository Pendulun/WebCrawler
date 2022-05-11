from timeit import default_timer as timer
from Crawler import Crawler
import logging
import utils
import sys

class UndefinedCommandError(Exception):
    pass

class ArgsWrongTypeError(Exception):
    pass

def printUsage():
    print("Usage: python main.py -s <SEEDS> -n <LIMIT> [-d]")
    exit(1)

def getConfigFromArgs(validCommands):
    
    argsConfig = getConfigDictTemplate()
    readAllCommands = False
    posCommandExpected = 1

    while not readAllCommands:
        if sys.argv[posCommandExpected] in validCommands:
                if sys.argv[posCommandExpected] == "-s":
                    
                    argsConfig['seedPathFile'] = sys.argv[posCommandExpected+1]
                    posCommandExpected += 2

                elif sys.argv[posCommandExpected] == "-n":
                    
                    try:
                        
                        argsConfig['LIMIT'] = int(sys.argv[posCommandExpected+1])

                    except ValueError as e:

                        raise ArgsWrongTypeError("Wrong value type for command '", 
                                            sys.argv[posCommandExpected],
                                            "'. type() == int expected but '",
                                            sys.argv[posCommandExpected+1],
                                            "' value was given") from e

                    else:
                        posCommandExpected += 2
                        
                elif sys.argv[posCommandExpected] == "-d":
                    argsConfig['debugMode'] = True
                    posCommandExpected += 1
        else:
            raise UndefinedCommandError("Unsupported command: ",sys.argv[posCommandExpected])
        
        if posCommandExpected >= len(sys.argv):
            readAllCommands = True
        
    return argsConfig

def getConfigDictTemplate():
    templateConfig = dict()
    templateConfig['seedPathFile'] = ""
    templateConfig['LIMIT'] = 0
    templateConfig['debugMode'] = False
    return templateConfig

if __name__ == "__main__":
    
    logging.basicConfig(level=logging.INFO, format='%(thread)d-%(threadName)s-%(levelname)s-%(message)s',
    filename="log.log", filemode="w")

    #logging.disable(logging.INFO)

    MINNUMARGS = 5

    if len(sys.argv) < MINNUMARGS:
        printUsage()
    else:
        VALIDCOMMANDS = ["-s", "-n", "-d"]
        configs = dict()
        try:
            configs = getConfigFromArgs(VALIDCOMMANDS)
        except (UndefinedCommandError, ArgsWrongTypeError) as e:
            utils.printErrorMessageAndExitWithErrorCode(e, 1)
        else:
            logging.info(f"Todos o comandos foram aceitos {configs}")

            NUMWORKERS = 110
            myCrawler = Crawler(configs['LIMIT'], NUMWORKERS, configs['debugMode'])
            try:
                start = timer()
                myCrawler.startCrawlingFromSeedsFile(configs['seedPathFile'])
                end = timer()
                totalTime = end - start
                print(f"Elapsed Time: {totalTime}")
                print(f"Num Of Pages Crawled : {myCrawler.pagesCrawled}")
            except FileNotFoundError as e:
                utils.printErrorMessageAndExitWithErrorCode(e, 1)