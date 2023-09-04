import csv
import shutil

from dataimport.analysis import Analysis
from dataimport.cli import entry_point
from dataimport.datasource import Datasource
from tests.util import TestDataimport


class MockAnalysis(Analysis):
    def __init__(self, source_id, source_file):
        super(MockAnalysis, self).__init__(source_id)
        self._file = source_file

    def entries(self):
        with open(self._file) as f:
            data = csv.reader(f)
            for v in data:
                yield v


class MockDatasource(Datasource):
    ANALYSES = [MockAnalysis]

    def fetch(self):
        local_copy = self.file_manager.file_path("origin.csv")
        shutil.copyfile('./tests/origin.csv', local_copy)
        self.log("Copied {x} to {y}".format(x="origin.csv", y=local_copy))

    def analyse(self):
        even_numbers = []
        with self.file_manager.input_file("origin.csv") as f:
            reader = csv.DictReader(f)
            for row in reader:
                for k, v in row.items():
                    if int(v) % 2 == 0:
                        even_numbers.append(v)

        self.log("Writing even numbers to {x}".format(x=self.file_manager.file_path("analysed.csv")))
        with self.file_manager.output_file("analysed.csv") as f:
            writer = csv.writer(f)
            writer.writerow(even_numbers)

    def analysis(self, analysis_class):
        if analysis_class != MockAnalysis:
            return None

        source_file = self.file_manager.file_path("analysed.csv")
        return MockAnalysis(self.id, source_file)


class TestDatasource(TestDataimport):
    mode = 'mockdatasource'

    def test_fetch(self):
        """
        Test only the fetch stage.
        """
        args = ['resolve', 'mockdatasource', '-s', 'fetch', '-o',  '-c', 'tests.mock_settings']
        result = self.runner.invoke(entry_point, args)

        # Expect successful execution
        self.assertEqual(result.exit_code, 0)
        # Expect the file to exist
        self.assertIsFile('origin.csv')
        # Expect the action to be logged
        self.assertRegex(result.output, r'Copied origin.csv to ')
        # Expect that no other action is logged
        self.assertNotRegex(result.output, r'Analysing datasource')

    def test_all(self):
        """
        Test the whole pipeline.
        """
        result = self.runner.invoke(entry_point, ['resolve', 'mockdatasource', '-c', 'tests.mock_settings'])

        # Expect successful execution
        self.assertEqual(result.exit_code, 0)
        # Expect the original datasource file to exist
        self.assertIsFile('origin.csv')
        # Expect also the analysed file to exist
        self.assertIsFile('analysed.csv')
