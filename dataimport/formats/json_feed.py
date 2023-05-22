from dataimport.format import Format
import json


class JSONFeed(Format):
    def entries(self):
        raise NotImplementedError()


class LineByLineJSON(JSONFeed):
    def __init__(self, path):
        super(LineByLineJSON, self).__init__()
        self._path = path

    def entries(self):
        with open(self._path) as f:
            for line in f:
                yield json.loads(line)