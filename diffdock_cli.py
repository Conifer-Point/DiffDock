import json
import logging
from argparse import ArgumentParser
from pathlib import Path

from diffdock_protocol import DiffDockProtocol
from diffdock_api import DiffDockApi, DiffDockOptions

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

# CLI version of DiffDockApi. Implemented for testing

def main(args):
    log.info(f"Running DiffDock CLI with {args}")
    # Prepare
    try:
        with open(args.input, 'r', encoding='utf8') as inf:
            inputJson = inf.read()
        request = DiffDockProtocol.Request.from_json(inputJson)
        options = DiffDockOptions()
        if args.workdir:
            options.work_dir = Path(args.workdir)
    except Exception as ex:
        log.exception("Exception preparing request")
        reportError(args, f"Exception preparing request {ex}")
        return

    # Execute
    try:
        responseObj = DiffDockApi.run_diffdock(request, options)
        writeResponse(args, responseObj)
    except Exception as ex:
        log.exception("Exception running diffdock")
        reportError(args, f"Exception running diffdock: {ex}")
    log.info(f"Done with DiffDock CLI")


def reportError(args, errMsg):
    responseObj = DiffDockProtocol.Response.makeError(errMsg)
    writeResponse(args, responseObj)

def writeResponse(args, responseObj):
    with open(args.output, 'w', encoding='utf8') as outf:
        outf.write(responseObj.to_json())

if __name__ == "__main__":
    arg_parser = ArgumentParser()
    arg_parser.add_argument('--input', type=str, help='json file with diffdock request', required=True)
    arg_parser.add_argument('--output', type=str, help='json file with diffdock response', required=True)
    arg_parser.add_argument('--workdir', type=str, help='working directory')
    args = arg_parser.parse_args()    
    main(args)
