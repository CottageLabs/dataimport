import os
import pathlib as pl
import shutil
import unittest

from click.testing import CliRunner

from tests.mock_settings import STORE_SCOPES


def latest_dir(entity: str) -> str:
    dir = STORE_SCOPES[entity]
    subdirs = sorted(os.listdir(dir), reverse=True)
    return os.path.join(dir, subdirs[0])


class TestDataimport(unittest.TestCase):
    mode = None

    @classmethod
    def setUpClass(cls):
        cls.runner = CliRunner()

    @classmethod
    def tearDownClass(cls) -> None:
        for scope in STORE_SCOPES:
            shutil.rmtree(STORE_SCOPES[scope], ignore_errors=True)

    def assertIsFile(self, filename: str):
        path = latest_dir(self.mode) + '/' + filename

        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))
