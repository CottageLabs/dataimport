from dataimport.target import Target


class Invenio(Target):
    """ Upload to an Invenio API endpoint """

    def prepare(self):
        pass

    def load(self):
        pass

    def cleanup(self):
        super(Invenio, self).cleanup()