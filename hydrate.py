"""Force Google Drive / OneDrive placeholder files to download to local cache.

Google Drive for desktop (and OneDrive Files On-Demand) present cloud files as
local paths, but the bytes aren't actually present until something reads them.
Reading even a single byte forces the OS to hydrate the full file synchronously.
We read a small chunk, which is enough to trigger the full download.
"""

import os


def hydrate(path, chunk_size=4096):
    """Force a cloud-backed file to download by reading a small chunk.

    Returns the path on success. Raises on I/O error.
    Note: the OS download is synchronous — this call blocks until the full
    file is available locally, even though we only read a few KB.
    """
    with open(path, "rb") as f:
        f.read(chunk_size)
    return path


def is_hydrated(path):
    """Best-effort check: returns True if the file appears to be locally cached.

    There's no perfectly reliable cross-provider API for this, so we fall back
    to checking that the file exists and has nonzero size. Call hydrate() if
    you need a guarantee.
    """
    try:
        return os.path.isfile(path) and os.path.getsize(path) > 0
    except OSError:
        return False