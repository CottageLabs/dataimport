class Analysis(object):
    def __init__(self, source_id):
        self._source_id = source_id

    @property
    def source(self):
        return self._source_id

    def entries(self):
        raise NotImplementedError()