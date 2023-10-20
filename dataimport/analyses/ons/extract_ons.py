import json
from typing import List

from dataimport.analysis import Analysis


class ExtractONSData(Analysis):
    pass


class ExtractONSDataFromJSON(ExtractONSData):
    def __init__(self, source_id, filepaths: List[str]):
        super(ExtractONSDataFromJSON, self).__init__(source_id)
        self._filepaths = filepaths

    def entries(self) -> str:
        output = []
        for filepath in self._filepaths:
            with open(filepath, "r") as f:
                obj = json.loads(f.read())
                output.append(obj)

        return output
