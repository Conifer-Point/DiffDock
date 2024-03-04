import asyncio
import json
import logging
import re
import os
from websockets.server import serve
import concurrent.futures
from argparse import ArgumentParser

from cp_diffdock_protocol import DiffDockProtocol
from cp_diffdock_api import DiffDockApi
from cp_ws_helpers import extractWsAppMessage, formWsAppMessage, formCompletedMessage

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('diffdock_service')

#
# This is a websocket server for servicing DiffDock requests.
# This listens on a port and places incoming json requests into a queue, serviced by a number of workers.
#

async def handleRequest(websocket, queue):
    """
    Receive a request and put it into the queue.
    Make sure we keep the websocket so we can send the response when docking finishes.
    """
    try:
        message = await websocket.recv()
    except Exception as ex:
        log.error(f"Exception reading from websocket: {ex}. Can't handle this request.")
        return

    log.info("Received a request.")
    # Optional special handling for messages from bfd-server or other Bioleap WsApps
    # requestId is non-empty if it is from a wsApp, and will need to be included in responses.
    requestId, cmdName, requestData = extractWsAppMessage(message)
    requestContext = websocket, requestId

    # Parse
    try:
        requestObj = DiffDockProtocol.Request.from_json(requestData)
    except:
        log.exception(f"Rejected a request:\nMessage: {message}\nrequestData: {requestData}")
        await sendError(requestContext, f"Invalid request")
        return

    # Enqueue
    log.info("handleRequest queuing a request...")
    try:
        # Put the request into the queue with a condition to resolve when it finishes.
        # Condition protects the websocket from closing early.
        condition = asyncio.Condition()
        await queue.put((requestObj, requestContext, condition))
        await sendStatus(requestContext, "Request received")
        async with condition:
            await condition.wait()
    except Exception as ex:
        log.exception(f"handleRequest encountered exception")
        await sendError(requestContext, f"Service exception: {ex}")


async def queueWorker(queue):
    """
    Handle docking requests from the queue
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        while True:
            # Wait for an item from the queue
            requestObj, requestContext, condition = await queue.get()

            log.info("worker running a request...")
            try:
                # Since this is async, but diffdock is not, it seems it is tricky.
                # run_in_executor seemed to do the trick.
                # We want to make sure that diffdock execution doesn't block other
                # connections from putting their requests into the queue.
                await sendStatus(requestContext, "Working request...")
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(executor, DiffDockApi.run_diffdock, requestObj)
                log.info("queueWorker resolved response")
                await sendResults(requestContext, response)
                log.info("queueWorker sent response.")
            except Exception as ex:
                log.exception(f"queueWorker encountered exception")
                await sendError(requestContext, f"Service exception: {ex}")

            # Notify the queue that the item has been processed
            queue.task_done()

            # Resolve the condition for the request handler
            async with condition:
                condition.notify_all()


async def main(host="localhost", port=9002, max_size=2**24, worker_count=5):
    """
    DiffDock websocket server main loop.
    Set up the queue, worker threads, and listen for connections.
    """
    queue = asyncio.Queue()
    workers = [asyncio.create_task(queueWorker(queue)) for _ in range(worker_count)]

    async def handlerWrapper(websocket, path):
        await handleRequest(websocket, queue)

    log.info(f"DiffDock service running with pid {os.getpid()}, listening at {host}:{port}...")
    start_server = serve(handlerWrapper, host, port, max_size=max_size)

    log.info(f"DiffDock ready.")
    try:
        await start_server
        await asyncio.Future()
    except KeyboardInterrupt:
        log.info("Received KeyboardInterrupt")

    # SHUTDOWN
    try:
        log.info("DiffDock service shutting down...let this finish naturally if you can")
        server.close()
        loop.run_until_complete(server.wait_closed())
        for worker in workers:
            worker.cancel()
    except:
        log.info("DiffDock service caught shutting down, will force termination")
    finally:
        log.info("DiffDock service finished.")
        os._exit(0)


async def sendToWebsocket(websocket, data):
    try:
        return await websocket.send(data)
    except Exception as ex:
        log.error(f"Exception encountered when writing to a websocket: {ex}")

async def sendPacket(requestContext, responseObj):
    websocket, requestId = requestContext
    if requestId:
        return await sendWsAppPacket(requestContext, responseObj)
    else:
        return await sendToWebsocket(websocket, responseObj.to_json())

async def sendWsAppPacket(requestContext, responseObj):
    """
    Bioleap WsApp (bfd-server) messages look like `#<request id> <cmd name> <message>`
    and a request must be terminated with a `completed` message.
    """
    websocket, requestId = requestContext
    messageType = responseObj.messageType
    responseCmd = None
    needsComplete = False
    if messageType == DiffDockProtocol.MessageType.ERROR:
        responseCmd = "diffdock-error"
        needsComplete = True
    elif messageType == DiffDockProtocol.MessageType.RESULTS:
        responseCmd = "diffdock-results"
        needsComplete = True
    elif messageType == DiffDockProtocol.MessageType.STATUS:
        responseCmd = "diffdock-status"

    payload = formWsAppMessage(requestId, responseCmd, responseObj.to_json())
    log.info(f'Sending {payload}')
    await sendToWebsocket(websocket, payload)
    if needsComplete:
        # wsApp messages need a `complete` at the end of the transaction
        log.info(f'Sending completed')
        completePayload = formCompletedMessage(requestId)
        return await sendToWebsocket(websocket, completePayload)

async def sendStatus(requestContext, status):
    responseObj = DiffDockProtocol.Response.makeStatus(status)
    return await sendPacket(requestContext, responseObj)

async def sendError(requestContext, error):
    responseObj = DiffDockProtocol.Response.makeError(error)
    return await sendPacket(requestContext, responseObj)

async def sendResults(requestContext, responseObj):
    return await sendPacket(requestContext, responseObj)


# ENTRYPOINT
arg_parser = ArgumentParser()
arg_parser.add_argument('--port', type=int, default=9002, help='Port to listen on')
arg_parser.add_argument('--worker_count', type=int, default=5, help='Number of worker threads to run')
args = arg_parser.parse_args()
log.info("Starting inference service...")
asyncio.run(main(port=args.port, worker_count=args.worker_count))
