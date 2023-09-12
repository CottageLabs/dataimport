from datetime import datetime


def log(message, source=None, update: bool = False):
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    message = str(message)

    if source is None:
        source = ""
    else:
        source += ":"

    if update:
        print("[" + timestamp + "] " + source + " " + message, end='\r')
    else:
        print("[" + timestamp + "] " + source + " " + message)
