import shutil
import unittest

from click.testing import CliRunner

from tests.mock_settings import STORE_SCOPES


class TestDataimport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.runner = CliRunner()

    @classmethod
    def tearDownClass(cls) -> None:
        for scope in STORE_SCOPES:
            shutil.rmtree(STORE_SCOPES[scope], ignore_errors=True)
