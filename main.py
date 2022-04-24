import sys

class UndefinedCommand(Exception):
    pass

class ArgsWrongType(Exception):
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

                        raise ArgsWrongType("Wrong value type for command '", 
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
            raise UndefinedCommand("Unsupported command: ",sys.argv[posCommandExpected])
        
        if posCommandExpected >= len(sys.argv):
            readAllCommands = True
        
    return argsConfig

def printErrorMessageAndExitWithErrorCode(exceptionRaised, errorCode):
    print("".join(exceptionRaised.args))
    exit(errorCode)

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
        except UndefinedCommand as e:
            printErrorMessageAndExitWithErrorCode(e, 1)
        except ArgsWrongType as e:
            printErrorMessageAndExitWithErrorCode(e, 1)
        else:
            print("Todos o comandos foram aceitos")
            print(configs)