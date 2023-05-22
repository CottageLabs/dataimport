class FormatNotSupported(Exception):
    def __init__(self, klazz):
        super(FormatNotSupported, self).__init__()
        self._klazz = klazz


class Format(object):
    pass
