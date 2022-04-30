import logging

def printErrorMessageAndExitWithErrorCode(exceptionRaised: Exception, errorCode: int):
    printJoinedErrorMessage(exceptionRaised)
    exit(errorCode)

def printJoinedErrorMessage(exceptionRaised: Exception):
    logging.exception(f"{type(exceptionRaised)} ","".join(exceptionRaised.args))

def getHostAndResourcesFromLink(link:str):
    host = getHostOfLink(link)
    resources = getResourcesFromLink(link)
    return host, resources

def getHostOfLink(link:str) -> str:
    """
    Assumes the link is in the format:
    https://host/resources
    """
    return link.split("/")[2]

def getResourcesFromLink(link: str) -> str:
    return f"/{'/'.join(link.split('/')[3:])}"

def threadOfHost(numThreads:int, host:str) -> int:
    return abs(hash(host)%numThreads)