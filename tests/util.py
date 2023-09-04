import unittest

from click.testing import CliRunner


class TestDataimport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.runner = CliRunner()
