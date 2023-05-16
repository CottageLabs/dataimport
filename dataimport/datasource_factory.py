from dataimport.lib import plugin


class DatasourceFactory(object):

    def __init__(self, config):
        self.config = config

    def get_datasource(self, name):
        ref = self.config.DATASOURCES.get(name)
        klazz = plugin.load_class(ref)
        return klazz(self.config, name)

    def get_all_datasources(self):
        all = []
        for name, ref in self.config.DATASOURCES.items():
            all.append(self.get_datasource(name))
        return all

    def get_all_datasource_names(self):
        return list(self.config.DATASOURCES.keys())