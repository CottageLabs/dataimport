from datetime import datetime
import os, shutil
from contextlib import contextmanager


class FileManager(object):
    def __init__(self, config, storage_id, instance=None, base_dir=None):
        self.config = config

        self._storage_id = storage_id

        self._dir = base_dir
        if self._dir is None:
            self._dir = config.STORE_SCOPES[storage_id]

        self._instance = instance
        if instance is None:
            self.current()

    def fresh(self):
        instance = datetime.strftime(datetime.utcnow(), self.config.DIR_DATE_FORMAT)
        self._instance = os.path.join(self._dir, instance)

    def current(self):
        dirs = []
        if not os.path.exists(self._dir):
            self.fresh()
            return

        for entry in os.listdir(self._dir):
            if os.path.isdir(os.path.join(self._dir, entry)):
                dirs.append(entry)

        if len(dirs) == 0:
            self.fresh()
            return

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

        keep_historic = self.config.STORE_KEEP_HISORIC[self._storage_id]

        if len(dirs) <= keep_historic:
            return

        dirs.sort(reverse=True)
        for remove in dirs[keep_historic:]:
            removing = os.path.join(self._dir, remove)
            shutil.rmtree(removing)