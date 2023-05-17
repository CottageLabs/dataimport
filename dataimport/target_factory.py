from dataimport.lib import plugin


class TargetFactory(object):

    def __init__(self, config):
        self.config = config

    def get_target(self, name):
        ref = self.config.TARGETS.get(name)
        klazz = plugin.load_class(ref)
        return klazz(self.config, name)

    def get_all_targets(self):
        all = []
        for name, ref in self.config.TARGETS.items():
            all.append(self.get_target(name))
        return all

    def get_all_target_names(self):
        return list(self.config.TARGETS.keys())