import os
import errno
import re
import logging
import threading
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List

from cp_diffdock_protocol import DiffDockProtocol
from cp_inference_module import InferenceArgs, RunInference

log = logging.getLogger('diffdock_api')

#
# DiffDock API - Wrapper for running DiffDock with DiffDockProtocol objects for requests and responses
#

class RequestIdGenerator:
    def __init__(self):
        self.count = 0
        self.lock = threading.Lock()

    def get_id(self):
        with self.lock:
            result = self.count
            self.count += 1
            return result
requestIdGenerator = RequestIdGenerator()

@dataclass
class PreppedComplexUnit:
    """
    A protein or ligand part of a docking job, including the file path.
    """
    name: str
    label: str = ""
    path: str = ""
    error: str = ""

    def ok(self):
        return not self.error

    def __iter__(self):
        # Specific about fields here so error isn't included when unpacking
        return iter([self.name, self.label, self.path])


@dataclass
class PreppedComplex:
    """
    A protein-ligand complex to be docked, including the protein and ligand parts.
    This contains the data for the DiffDock csv input file.
    """
    name: str
    protein: PreppedComplexUnit
    ligand: PreppedComplexUnit

    def __iter__(self):
        # Specific about fields here to avoid converting protein and ligand to dict
        return iter([self.name, self.protein, self.ligand])


@dataclass
class PreppedRequest:
    """
    A DiffDock request after preparing the pdb, sdf, and csv input files.
    """
    csv_file: str
    out_dir: str
    complex_infos: List[PreppedComplex]

    def __iter__(self):
        # Specific about fields here to avoid converting complex_infos to dict
        return iter([self.csv_file, self.out_dir, self.complex_infos])

@dataclass
class DiffDockOptions:
    work_dir: Path

    @staticmethod
    def make(baseDir=None, requestId=None):
        # get_id needs to run every time, so don't use default arguments
        if baseDir is None:
            baseDir = f"/tmp/diffdock{os.getpid()}"
        if requestId is None:
            requestId = requestIdGenerator.get_id()
        work_dir = Path(baseDir) / f"dock_{requestId}"
        return DiffDockOptions(
            work_dir=work_dir
        )


class DiffDockSetupException(Exception):
    pass


class DiffDockApi:
    """
    Wrapper for running DiffDock with DiffDockProtocol objects for requests and responses.
    """
    @staticmethod
    def run_diffdock(request: DiffDockProtocol.Request, options: DiffDockOptions=None) -> DiffDockProtocol.Response:
        try:
            log.info("diffdock preparing request...")
            preppedRequest = DiffDockApi.prepare_request(request, options)

            args = InferenceArgs({
                "protein_ligand_csv": preppedRequest.csv_file,
                "out_dir": preppedRequest.out_dir,
                "add_hs": request.add_hs,
                "keep_hs": request.keep_hs,
                "keep_src_3d": request.keep_src_3d,
                "samples_per_complex": request.samples_per_complex,
            })
            log.info("diffdock running inference...")
            RunInference(args)
            response = DiffDockApi.process_results(preppedRequest)
            log.info("diffdock prepared response.")
            return response
        except Exception as ex:
            return DiffDockProtocol.Response.makeError(f"DiffDock failed. {ex}")


    @staticmethod
    def prepare_request(docking_request: DiffDockProtocol.Request, options: DiffDockOptions=None) -> PreppedRequest:
        """
        DiffDock works on files in directories. This function prepares the pdb, sdf, and csv files needed
        to fulfill the DiffDock request.
        """

        # Create temp directory for this run
        if options is None:
            options = DiffDockOptions.make()

        work_dir = options.work_dir
        try:
            os.makedirs(work_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise DiffDockSetupException(f"Could not prepare work directory, errcode {e.errno} ({e.strerror})")

        # Write and remember pdb files
        protein_entries: List[PreppedComplexUnit] = []
        for protein_i, protein in enumerate(docking_request.proteins):
            protein_name, proteinData = protein
            protein_label = f"protein{protein_i}"
            pdb_file = work_dir / f"{protein_label}.pdb"
            try:
                with open(pdb_file, 'w') as f:
                    f.write(proteinData)
                protein_entries.append(PreppedComplexUnit(protein_name, protein_label, pdb_file))
            except IOError as e:
                msg = f"DiffDock failed to write a protein file: {e.strerror}"
                protein_entries.append(PreppedComplexUnit(protein_name, error=msg))

        # Write and remember ligand files
        ligand_entries: List[PreppedComplexUnit] = []
        for ligand_i, ligand in enumerate(docking_request.ligands):
            ligand_name, ligandData = ligand
            ligand_label = f"ligand{ligand_i}"
            sdf_file = work_dir / f"{ligand_label}.sdf"
            try:
                with open(sdf_file, 'w') as f:
                    f.write(ligandData)
                ligand_entries.append(PreppedComplexUnit(ligand_name, ligand_label, sdf_file))
            except IOError as e:
                msg = f"DiffDock failed to write ligand file: {e.strerror}"
                ligand_entries.append(PreppedComplexUnit(ligand_name, error=msg))

        # Write csv with docking entries for each protein+ligand complex
        docking_entries: List[PreppedComplex] = []
        for protein_entry in protein_entries:
            for ligand_entry in ligand_entries:
                complex_name = f"{protein_entry.label}-{ligand_entry.label}"
                docking_entries.append(PreppedComplex(complex_name, protein_entry, ligand_entry))

        csv_file = work_dir / "bmaps_diffdock.csv"
        csv_content = DiffDockApi.make_request_csv(docking_entries)
        if csv_content:
            try:
                with open(csv_file, 'w') as f:
                    f.write(csv_content)
            except IOError as e:
                raise DiffDockSetupException(f"Could not write diffdock csv file: {e.strerror}")
        else:
            log.info(f"Couldn't prepare diffdock csv. Entries: {docking_entries}")
            raise DiffDockSetupException("No suitable complexes")

        out_dir = work_dir / "results"
        return PreppedRequest(csv_file, out_dir, docking_entries)

    @staticmethod
    def process_results(preppedRequest: PreppedRequest) -> List[DiffDockProtocol.Response]:
        """
        DiffDock produces sdf files for each docked pose, in directories by complex name.
        This function gathers the resulting sdfs for each complex to be returned by the DiffDock API.
        """
        allComplexResults = []

        for complexInfo in preppedRequest.complex_infos:
            # Find the output directory for this complex
            complexName, proteinUnit, ligandUnit = complexInfo
            proteinName = proteinUnit.name
            ligandName = ligandUnit.name
            resultDir = preppedRequest.out_dir / complexName
            if not os.path.isdir(resultDir):
                noPosesError = "No poses generated."
                if not proteinUnit.ok():
                    noPosesError += f" protein prep failure: {proteinUnit.error}."
                if not ligandUnit.ok():
                    noPosesError += f" ligand prep failure: {ligandUnit.error}."
                allComplexResults.append(DiffDockProtocol.Result(proteinName, ligandName, error=noPosesError))
                continue

            # Process the sdfs in the output directory
            poses = []
            for dir_entry in os.scandir(resultDir):
                filepath = dir_entry.path
                filename = os.path.basename(filepath)

                # Regex to get rank and confidence
                rankRegex = re.compile(r"rank(\d+)(_confidence(-?[0-9.]+))?\.sdf")
                match = rankRegex.match(filename)

                if match:
                    rankStr = match.group(1)
                    confidenceStr = match.group(3)
                    rankInt = int(rankStr)
                    confidenceFloat = -1
                    if confidenceStr:
                        confidenceFloat = float(confidenceStr)
                    with open(filepath, 'r') as f:
                        fileContent = f.read()
                    poses.append(DiffDockProtocol.Pose(filename, fileContent, rankInt, confidenceFloat))

            poses.sort(key=lambda pose: pose.rank)
            allComplexResults.append(DiffDockProtocol.Result(proteinName, ligandName, poses=poses))

        return DiffDockProtocol.Response.makeResults(allComplexResults)

    @staticmethod
    def make_request_csv(entries: List[PreppedComplex]) -> str:
        header = "complex_name,protein_path,ligand_description,protein_sequence\n"
        csv = ""
        for entry in entries:
            complex_name, protein_unit, ligand_unit = entry
            log.info(f"Working with {complex_name=} {type(protein_unit)} {type(ligand_unit)}")
            protein_name, protein_label, protein_file = protein_unit
            ligand_name, ligand_label, ligand_file = ligand_unit
            if protein_unit.ok() and ligand_unit.ok():
                csv += f"{complex_name},{protein_file},{ligand_file},\n"
        return "" if csv == "" else header + csv
