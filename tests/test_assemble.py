import csv
import json

from dataimport.cli import entry_point
from dataimport.format import Format
from dataimport.formats.json_feed import JSONFeed, LineByLineJSON
from dataimport.product import Product
from tests.test_resolve import MockAnalysis
from tests.util import TestDataimport


class CsvFormat(Format):
    def __init__(self, csv_file):
        self._csv_file = csv_file

    def read(self):
        with open(self._csv_file) as f:
            data = csv.reader(f)
            for v in data:
                yield v


class MockProduct(Product):
    FORMATS = [CsvFormat, JSONFeed]

    def get_format(self, format_class):
        if format_class != JSONFeed:
            return None

        path = self.file_manager.file_path("mapping.json")
        return LineByLineJSON(path)

    def analyse(self):
        """
        This method takes the even numbers from the datasource analyses and
        doubles each number.
        """
        self.log("Preparing to double even numbers")

        sources = self.get_sources()
        syndata = []
        for ds in sources:
            if ds.provides_analysis(MockAnalysis):
                syndata.append(ds.analysis(MockAnalysis))

        even_numbers = []
        for group in syndata:
            for entry in group.entries():
                even_numbers.append([int(i) * 2 for i in entry])

        self.log("Writing even numbers to {x}".format(x=self.file_manager.file_path("analysed.csv")))
        with self.file_manager.output_file("analysed.csv") as f:
            writer = csv.writer(f)
            writer.writerow(even_numbers[0])

    def assemble(self):
        """
        This method writes the numbers as a json file.
        """
        self.log("Assembling json data")

        numbers = []

        with self.file_manager.input_file("analysed.csv") as f:
            reader = csv.reader(f)
            for row in reader:
                numbers.append([int(i) for i in row])

        self.log("Writing data to json {x}".format(x=self.file_manager.file_path("mapping.json")))
        with self.file_manager.output_file("mapping.json") as f:
            f.write(json.dumps({'numbers': numbers}))


class TestAssemble(TestDataimport):
    mode = 'mockproduct'

    def test_analyse(self):
        """
        Test the analyse stage
        """
        args = ['assemble', 'mockproduct', '-s', 'analyse', '-o',  '-c', 'tests.mock_settings']
        result = self.runner.invoke(entry_point, args)

        # Expect successful execution
        self.assertEqual(result.exit_code, 0)
        # Expect the analysed file from the resolve stage to be present
        self.assertIsFile('analysed.csv')
        # Expect the action to be logged
        self.assertRegex(result.output, r'Writing even numbers to ')

    def test_assemble(self):
        """
        Test the assemble stage
        """
        args = ['assemble', 'mockproduct', '-s', 'assemble', '-o', '-c', 'tests.mock_settings']
        result = self.runner.invoke(entry_point, args)

        # Expect successful execution
        self.assertEqual(result.exit_code, 0)
        # Expect the analysed file from the resolve stage to be present
        self.assertIsFile('analysed.csv')
        # Expect the assembled file to be present
        self.assertIsFile('mapping.json')
        # Expect the actions to be logged
        self.assertRegex(result.output, r'Assembling json data')
        self.assertNotRegex(result.output, r'Writing even numbers to ')

    def test_all(self):
        """
        Test the whole pipeline.
        """
        args = ['assemble', 'mockproduct', '-c', 'tests.mock_settings']
        result = self.runner.invoke(entry_point, args)

        # Expect successful execution
        self.assertEqual(result.exit_code, 0)
        # Expect the analysed file from the resolve stage and assembled to be present
        self.assertIsFile('analysed.csv')
        self.assertIsFile('mapping.json')
        # Expect preceding actions to be logged
        self.assertRegex(result.output, r"Gathering data for 'mockproduct'")
        self.assertRegex(result.output, r"Analysing datasource 'mockdatasource'")
        self.assertRegex(result.output, r'Writing even numbers to ')
        self.assertRegex(result.output, r'Assembling json data')
