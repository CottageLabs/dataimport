from datetime import datetime
import os, shutil
from contextlib import contextmanager


class FileManagerException(Exception):
    pass


class FileManager(object):
    def __init__(self, config, storage_id, instance=None, base_dir=None):
        self.config = config

        self._storage_id = storage_id

        self._dir = base_dir
        if self._dir is None:
            self._dir = config.STORE_SCOPES[storage_id]

        self._instance = instance
        if instance is None:
            self.current(make_fresh=True)

    @property
    def current_instance_name(self):
        return self._instance

    def list_ons_files(self, contains: str = None) -> list[str]:
        if contains is None:
            contains = self.config.ORIGIN_SUFFIX
        return [f for f in os.listdir('/home/jabbi/PycharmProjects/dataimport/databases/datasources/ons/2023-08-30_1321') if contains in f]

    def list_files(self, contains: str = None) -> list[str]:
        if contains is None:
            contains = self.config.ORIGIN_SUFFIX
        return [f for f in os.listdir(self.current_instance_name) if contains in f]

    def fresh(self):
        instance = datetime.strftime(datetime.utcnow(), self.config.DIR_DATE_FORMAT)
        self._instance = os.path.join(self._dir, instance)
        os.makedirs(self._instance, exist_ok=True)

    def current(self, make_fresh=False):
        dirs = []
        if not os.path.exists(self._dir):
            if make_fresh:
                self.fresh()
                return
            else:
                raise FileManagerException("Current directory does not exist, and make_fresh is False")

        for entry in os.listdir(self._dir):
            if os.path.isdir(os.path.join(self._dir, entry)):
                dirs.append(entry)

        if len(dirs) == 0:
            if make_fresh:
                self.fresh()
                return
            else:
                raise FileManagerException("Current directory does not exist, and make_fresh is False")

        dirs.sort(reverse=True)
        self._instance = os.path.join(self._dir, dirs[0])

    def activate(self):
        os.makedirs(self._instance, exist_ok=True)

    def file_path(self, filename):
        self.activate()
        return os.path.join(self._instance, filename)

    @contextmanager
    def output_file(self, filename, mode="w"):
        self.activate()
        path = self.file_path(filename)
        with open(path, mode) as f:
            yield f

    @contextmanager
    def input_file(self, filename, mode="r"):
        self.activate()
        path = self.file_path(filename)
        with open(path, mode) as f:
            yield f

    def current_dir_created(self):
        dirs = []
        if not os.path.exists(self._dir):
            return False

        for entry in os.listdir(self._dir):
            if os.path.isdir(os.path.join(self._dir, entry)):
                dirs.append(entry)

        if len(dirs) == 0:
            return False

        dirs.sort(reverse=True)
        return datetime.strptime(dirs[0], self.config.DIR_DATE_FORMAT)

    def cleanup(self):
        if not os.path.exists(self._dir):
            return

        dirs = []
        for entry in os.listdir(self._dir):
            if os.path.isdir(os.path.join(self._dir, entry)):
                dirs.append(entry)

        keep_historic = self.config.STORE_KEEP_HISTORIC[self._storage_id]

        if len(dirs) <= keep_historic:
            return

        dirs.sort(reverse=True)
        for remove in dirs[keep_historic:]:
            removing = os.path.join(self._dir, remove)
            shutil.rmtree(removing)