import os


def get_secret(key):
    """ Get a secret from an environment variable or contents of a file """
    var = os.environ.get(key, None)
    if var is not None:
        return var

    # Fallback to pulling from a file
    if not os.path.exists(key):
        raise Exception("Secrets file {x} does not exist".format(x=key))

    with open(key) as f:
        contents = f.read().strip()

    if not contents:
        raise Exception("Secrets file {x} was empty".format(x=key))
    return contents
