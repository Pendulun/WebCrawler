import sys
from Crawler import Crawler
import utils

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

    MINNUMARGS = 5

    if len(sys.argv) < MINNUMARGS:
        printUsage()
    else:
        VALIDCOMMANDS = ["-s", "-n", "-d"]
        configs = dict()
        try:
            configs = getConfigFromArgs(VALIDCOMMANDS)
        except UndefinedCommandError as e:
            utils.printErrorMessageAndExitWithErrorCode(e, 1)
        except ArgsWrongTypeError as e:
            utils.printErrorMessageAndExitWithErrorCode(e, 1)
        else:
            print("Todos o comandos foram aceitos")
            print(configs)
            myCrawler = Crawler(configs['LIMIT'])
            try:
                myCrawler.startCrawlingFromSeedsFile(configs['seedPathFile'])
            except FileNotFoundError as e:
                utils.printErrorMessageAndExitWithErrorCode(e, 1)