import re


def clean_filename(filename):
    m = re.match(r"(?:.*/)*([^.]+).*", filename)
    if m and m.group(1):
        return m.group(1)
    return filename
