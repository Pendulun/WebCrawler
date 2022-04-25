def printErrorMessageAndExitWithErrorCode(exceptionRaised, errorCode):
    printJoinedErrorMessage(exceptionRaised)
    exit(errorCode)

def printJoinedErrorMessage(exceptionRaised):
    print(f"{type(exceptionRaised)} ","".join(exceptionRaised.args))