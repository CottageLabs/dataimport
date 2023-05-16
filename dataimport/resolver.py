from dataimport.file_manager import FileManager
from dataimport import logger

from datetime import datetime, timedelta


class Resolver(object):
    RESOLVE_PIPELINE = ["fetch", "analyse"]

    def __init__(self, config):
        self.config = config

    def log(self, msg):
        logger.log(msg, "RESOLVER")

    def resolve(self, datasources, force_update=False, stages=False):
        if not stages:
            stages = self.RESOLVE_PIPELINE

        for datasource in datasources:
            for stage in stages:
                if stage == "fetch":
                    self.fetch(datasource, force_update)
                elif stage == "analyse":
                    self.analyse(datasource)

    def fetch(self, datasource, force_update=False):
        if force_update or self.requires_update(datasource):
            datasource.file_manager.fresh()
            datasource.fetch()
            datasource.file_manager.cleanup()

    def analyse(self, datasource):
        datasource.analyse()

    def _max_age(self, datasource):
        return self.config.RESOLVER_MAX_AGE.get(datasource.id, 0)

    def requires_update(self, datasource):
        fm = FileManager(self.config, datasource.id)
        current = fm.current_dir_created()
        if current:
            return current + timedelta(seconds=self._max_age(datasource)) < datetime.utcnow()
        return True