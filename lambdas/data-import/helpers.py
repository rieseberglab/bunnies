import logging
import hashlib
import threading
import time

log = logging.getLogger(__name__)

class ProgressPercentage(object):
    def __init__(self, size=-1, logprefix="", min_interval_s=5.0, logger=log):
        self._size = size
        self._logprefix = logprefix
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._last_update = 0
        self._update_interval = min_interval_s
        self._logger = logger
        self._start_time = 0

    def log_progress(self):
        prog_siz = self._seen_so_far // (1024*1024)
        elapsed = self._last_update - self._start_time
        if self._size >= 0:
            prog_pct = self._seen_so_far / (self._size + 1) * 100
            self._logger.info("%s  progress %6d / %d MiB (%5.2f%%) elapsed %8.3f", self._logprefix,
                              prog_siz, (self._size + 512*1024) // (1024*1024), prog_pct, elapsed)
        else:
            self._logger.info("%s  progress %6d / -- MiB   --  elapsed %5.2f", self._logprefix, prog_siz, elapsed)

    def __call__(self, bytes_amount):
        with self._lock:
            if self._start_time == 0: self._start_time = time.time()
            self._seen_so_far += bytes_amount
            now = time.time() if self._update_interval > 0 else 0
            if self._update_interval <= 0 or (self._last_update + self._update_interval < now):
                self._last_update = now
                self.log_progress()

class HashingReader(object):
    """whenever data is read from this object, a corresponding amount of data
       is read from the source, and hashes are computed over this data
    """
    def __init__(self, sourcefp, algorithms=("md5", "sha1")):
        self._sourcefp = sourcefp
        self._hashers = []
        self._pos = 0

        for algo in algorithms:
            self._hashers.append(getattr(hashlib, algo)())

    def hexdigests(self):
        """ returns the hexdigests of all data that has gone through so far.
            { algoname: hexdigest, ...}
        """
        return { o.name: o.hexdigest() for o in self._hashers }

    def __getattr__(self, attr):
        return getattr(self._sourcefp, attr)

    def read(self, size=-1):
        chunk = self._sourcefp.read(size)
        self._pos += len(chunk)
        for obj in self._hashers:
            obj.update(chunk)
        return chunk

    def seekable(self, *args):
        return False

    def seek(self, *args, **kwargs):
        raise OSError("Not seekable/truncatable")

    def tell(self, *args, **kwargs):
        return self._pos

    def tuncate(self, *args, **kwargs):
        raise OSError("Not seekable/truncatable")
