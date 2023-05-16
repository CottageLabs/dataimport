from dataimport.analysis import Analysis
import csv

class Titles(Analysis):
    pass


class TitlesFromCSV(Titles):
    def __init__(self, source_id, filepath):
        super(Titles, self).__init__(source_id)
        self._filepath = filepath

    def entries(self):
        with open(self._filepath, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                yield row