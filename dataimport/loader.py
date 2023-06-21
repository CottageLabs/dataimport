from dataimport.file_manager import FileManager
from dataimport import logger

from datetime import datetime, timedelta
from dataimport.assembler import Assembler
from dataimport.target_factory import TargetFactory


class Loader(object):
    LOAD_PIPELINE = ["assemble", "prepare", "load"]

    def __init__(self, config):
        self.config = config

    def log(self, msg):
        logger.log(msg, "LOADER")

    def load(self, targets, force_update=False, stages=False):
        if not stages:
            stages = self.LOAD_PIPELINE

        # Note that in the loader we go stage by stage rather than target
        # by target, in order to run the final load stage as close together
        # as possible for all targets
        for stage in stages:
            for target in targets:
                if stage == "assemble":
                    self.assemble(target, force_update)
                elif stage == "prepare":
                    self.prepare(target)
                elif stage == "load":
                    self.loads(target)

    def assemble(self, target, force_update=False):
        self.log("Assembling products for '{x}'".format(x=target.id))
        target.assemble(force_update=force_update)

    def prepare(self, target):
        target.file_manager.fresh()
        self.log("Starting fresh preparation for '{x}' in {y}".format(x=target.id, y=target.file_manager.current_instance_name))
        target.prepare()
        target.cleanup()

    def loads(self, target):
        target.file_manager.current(make_fresh=True)
        self.log("Loading target '{x}' from {y}".format(x=target.id, y=target.file_manager.current_instance_name))
        target.load()
        target.cleanup()