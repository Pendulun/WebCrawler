import logging
from url_normalize import url_normalize

def printErrorMessageAndExitWithErrorCode(exceptionRaised: Exception, errorCode: int):
    printJoinedErrorMessage(exceptionRaised)
    exit(errorCode)

def printJoinedErrorMessage(exceptionRaised: Exception):
    logging.exception(f"{type(exceptionRaised)} ","".join(exceptionRaised.args))

def getHostAndResourcesFromLink(link:str):
    host = getHostOfLink(link)
    resources = getResourcesFromLink(link)
    return host, resources

def getHostWithSchemaAndResourcesFromLink(link:str):
    normalizedLink = normalizeLinkIfCan(link)
    hostWithSchema = getHostWithSchemaOfLink(normalizedLink)
    resources = getResourcesFromLink(normalizedLink)
    return hostWithSchema, resources

def normalizeLinkIfCan(link:str) -> str:
    newLink = ""
    try:
        newLink = url_normalize(link)
    except:
        newLink = link
    
    return newLink

def getHostOfLink(link:str) -> str:
    """
    Assumes the link is in the format:
    https://host/resources
    """
    return link.split("/")[2]

def getHostWithSchemaOfLink(link:str) -> str:
    schemaAndHost = ""
    try:
        schemaAndHostParts = link.split("/")[:3]
        schemaAndHost = f"{schemaAndHostParts[0]}//{schemaAndHostParts[2]}"
    except Exception as e:
        logging.exception(f"ERROR for link {link}:\n{e}")

    return schemaAndHost

def getResourcesFromLink(link: str) -> str:
    return f"/{'/'.join(link.split('/')[3:])}"

def threadOfHost(numThreads:int, host:str) -> int:
    return abs(hash(host)%numThreads)

def getCompleteLinkFromHostAndResource(host:str, resource:str) -> str:
        completeLink = f"{host}{resource}"
        return completeLink