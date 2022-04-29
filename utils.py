def printErrorMessageAndExitWithErrorCode(exceptionRaised, errorCode):
    printJoinedErrorMessage(exceptionRaised)
    exit(errorCode)

def printJoinedErrorMessage(exceptionRaised):
    print(f"{type(exceptionRaised)} ","".join(exceptionRaised.args))

def getHostAndResourcesFromLink(link):
    host = getHostOfLink(link)
    resources = getResourcesFromLink(link)
    return host, resources

def getHostOfLink(link):
    """
    Assumes the link is in the format:
    https://host/resources
    """
    return link.split("/")[2]

def getResourcesFromLink(link):
    return f"/{'/'.join(link.split('/')[3:])}"

def threadOfHost(numThreads, host):
    return abs(hash(host)%numThreads)