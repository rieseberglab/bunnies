import logging
import hashlib
import threading
import time
import io
import base64
import concurrent.futures

log = logging.getLogger(__name__)

class ProgressPercentage(object):
    def __init__(self, size=-1, logprefix="", min_interval_s=5.0, logger=log):
        self._size = size
        self._logprefix = logprefix
        self._pos = 0
        self._lock = threading.Lock()
        self._last_update = 0
        self._last_update_pos = 0
        self._update_interval = min_interval_s
        self._logger = logger
        self._start_time = 0

    def log_progress(self, as_of=None):
        prog_siz = self._pos // (1024*1024)
        now = as_of if as_of is not None else time.time()
        elapsed = now - self._start_time

        delta_t = now - self._last_update
        delta_x = self._pos - self._last_update_pos
        speed_mb = delta_x / (1024*1024*(delta_t+0.001))

        if self._size >= 0:
            prog_pct = self._pos / (self._size + 1) * 100
            self._logger.info("%s  progress %6d / %d MiB (%5.2f%%) %8.3fMB/s  elapsed %8.3f", self._logprefix,
                              prog_siz, (self._size + 512*1024) // (1024*1024), prog_pct, speed_mb, elapsed)
        else:
            self._logger.info("%s  progress %6d / -- MiB   --  %8.3fMB/s  elapsed %5.2f", self._logprefix, prog_siz,
                              speed_mb, elapsed)

    def __call__(self, bytes_amount):
        with self._lock:
            if self._start_time == 0: self._start_time = time.time()
            self._pos += bytes_amount
            now = time.time() if self._update_interval > 0 else 0
            if self._update_interval <= 0 or (self._last_update + self._update_interval < now):
                speed = self.log_progress(as_of=now)
                self._last_update_pos = self._pos
                self._last_update = now

class HashingReader(object):
    """whenever data is read from this object, a corresponding amount of data
       is read from the source, and hashes are computed over this data
    """
    def __init__(self, sourcefp, algorithms=("md5", "sha1"), progress_callback=None):
        self._sourcefp = sourcefp
        self._hashers = []
        self._pos = 0
        self._progress_callback = progress_callback

        for algo in algorithms:
            self._hashers.append(getattr(hashlib, algo)())
        if len(self._hashers):
            self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self._hashers))
        else:
            self._executor = None
        self._futures = []

    @property
    def progress_callback(self):
        return self._progress_callback

    @progress_callback.setter
    def progress_callback(self, progress):
        self._progress_callback = progress

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

        # notify
        if self._progress_callback:
            self._progress_callback(len(chunk))

        # drain previous chunk
        if self._futures:
            for future in self._futures:
                future.result()
            self._futures = None

        # asynchronously hash
        if chunk and self._executor:
            self._futures = [self._executor.submit(obj.update, chunk) for obj in self._hashers]

        return chunk

    def close(self):
        if self._executor:
            self._executor.shutdown(wait=True)
            self._executor = None
        return self._sourcefp.close()

    def seekable(self, *args):
        return False

    def seek(self, *args, **kwargs):
        raise OSError("Not seekable/truncatable")

    def tell(self, *args, **kwargs):
        return self._pos

    def tuncate(self, *args, **kwargs):
        raise OSError("Not seekable/truncatable")


def yield_in_chunks(fp, chunk_size_bytes):
    while True:
        chunk = fp.read(chunk_size_bytes)
        if not chunk:
            break
        yield chunk

def hex2b64(hexstr):
    if len(hexstr) % 2 != 0:
        raise ValueError("Invalid hexstring")
    hexbits = bytes([ (int(hexstr[i], 16) << 4) + int(hexstr[i+1], 16) for i in range(0, len(hexstr), 2) ])
    return base64.b64encode(hexbits).decode('ascii')
