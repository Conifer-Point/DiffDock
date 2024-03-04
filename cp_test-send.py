import json
import time
from argparse import ArgumentParser, BooleanOptionalAction
from websockets.sync.client import connect
from dataclasses import dataclass

from cp_ws_helpers import extractWsAppMessage, formWsAppMessage

@dataclass
class Expectations:
    hasResults: bool = False
    errorMessage: str = ""
    resultCount: int = -1
    totalPoseCount: int = -1

    @staticmethod
    def makeError(msg=""):
        return Expectations(hasResults=False, errorMessage=msg, resultCount=0)
    @staticmethod
    def makeResults(resultCount=-1, totalPoseCount=-1):
        return Expectations(hasResults=True, resultCount=resultCount, totalPoseCount=totalPoseCount)

def main():
    print(f'Running test-send with {args}')
    if args.run_tests:
        runTests(args)
    else:
        if args.expectError:
            expectations = Expectations.makeError()
        elif args.expectResultCount is not None or args.expectTotalPoseCount is not None:
            expectations = Expectations.makeResults()
            if args.expectResultCount is not None:
                expectations.resultCount = args.expectResultCount
            if args.expectTotalPoseCount is not None:
                expectations.totalPoseCount = args.expectTotalPoseCount
        else:
            expectations = None
        sendATestFile(args, args.requestFile, expectations)

def sendATestFile(args, filename, expectations=None):
    with open(filename, 'r', encoding='utf8') as reqFile:
        requestData = reqFile.read()
    return sendATest(args, requestData, expectations)

def sendATest(args, requestData, expectations=None):
    wsUrl = f'ws://{args.host}:{args.port}'
    start = time.time()
    with connect(wsUrl) as websocket:
        websocket.send(getTestPayload(args, requestData))
        allResponses = []

        while True:
            response = websocket.recv()
            print(response)
            requestId, cmdName, responseData = extractWsAppMessage(response)
            responseObj = json.loads(responseData)
            allResponses.append(responseObj)
            messageType = responseObj.get("messageType")
            if messageType == "status":
                print(f"Received status: {responseObj['message']}, still waiting for error or results.")
                continue
            else:
                print(f"Received {messageType}. Finished with this request.")
                finalResponse = responseObj
                if args.like_wsapps:
                    # WsApps send the final `completed` message
                    response = websocket.recv()
                    print(response)
                break

        end = time.time()
        print(f"Elapsed time = {end-start} sec")

        if expectations:
            if expectations.hasResults:
                assert finalResponse['messageType'] == 'results'
            else:
                assert finalResponse['messageType'] == 'error'
            if expectations.resultCount > -1:
                assert expectations.resultCount == len(finalResponse['results'])
            if expectations.totalPoseCount > -1:
                totalPoseCount = sum(
                    [ len(result['poses']) for result in finalResponse['results'] ]
                )
                assert expectations.totalPoseCount == totalPoseCount
            if expectations.errorMessage:
                assert expectations.errorMessage == finalResponse['error']
        # Done with expectations

        return allResponses, finalResponse, start, end

count = 0
def getTestPayload(args, requestData):
    if not args.like_wsapps:
        return requestData

    # Format request like wsapp: `<request id> <cmd name> <message>`
    global count
    cmdName = 'test-send-diffdock-request'
    requestId = f'#{cmdName}{count}'
    count += 1
    return formWsAppMessage(requestId, cmdName, requestData)

def runTests(args):
    testCases = [
        # filename, expectations
        ('bad_pdb', Expectations.makeError()),
        ('empty', Expectations.makeError()),
        ('incomplete', Expectations.makeError()),
        ('malformed', Expectations.makeError()),
        ('nothing', Expectations.makeResults(0)),
        ('request', Expectations.makeResults(1, 11)),
        ('request1.5', Expectations.makeResults(2, 11)), # 10 poses with confidences, plus a repeat of rank1
        ('request-slash', Expectations.makeResults(1, 11)),
        ('request3', Expectations.makeResults(6, 66)),

    ]
    for filename, expectations in testCases:
        sendATestFile(args, f'cp_tests/{filename}.json', expectations)
    print("DONE WITH TESTS")


parser = ArgumentParser()
parser.add_argument('--requestFile', type=str, help='json file containing the request to send', default='cp_tests/request.json')
parser.add_argument('--port', type=int, help='websocket port to connect to', default=9007)
parser.add_argument('--host', type=str, help='websocket host to connect to', default='localhost')
parser.add_argument('--expectError', action=BooleanOptionalAction, type=bool, help='Whether the request is expected to produce an error')
parser.add_argument('--expectResultCount', type=int, help='Whether the request is expected to produce an error')
parser.add_argument('--expectTotalPoseCount', type=int, help='Whether the request is expected to produce an error')
parser.add_argument('--run-tests', action=BooleanOptionalAction, type=bool, help='run all tests')
parser.add_argument('--like-wsapps', action=BooleanOptionalAction, type=bool, help='Add Bioleap-style requestids')
args = parser.parse_args()

main()
