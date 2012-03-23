import streaming_httplib2 as httplib2
import time
import os
import re
import errno
import threading
import fcntl
import logging

"""
Implements a multi process cache suitable for use with httplib2.
When a cache file is searched for by httplib2, and it does not exist, it is created and locked.
So, if a concurrent process is trying to open it, it will exists, so he will try to lock it first.
He then will be blocked until the first process writes it and unlock it.
This prevent the same file to be downloaded several times. 
It ensures too that the data that is written consistently (atomic write as the file is locked).
The same applies when updating the cache, or deleting this : it will be cross-process atomic.

You can just use the urlopen function to create 

Using the nice httplib2 caching code, it creates a multi process web cache.

Using distributed file systems such as GlusterFS, without duplication, as it is not useful for a cache,
and with a small layer of retry, when glusterfs is partially down for example.
a multi-machine web cache can be created easily.


"""


def urlopen(url, cache_dir, path_schema = [(0,2), (2,4)], create_cache_dirs = False):
    """Open a single url, a return the response info and content stream.
       For path_schema documentation, see below, in DistributedFileCache constructor.
       Be careful, ssl_certificate_validation is disactivated."""
    c = DistributedFileCache(cache_dir, path_schema = path_schema, 
            create = create_cache_dirs)
    h = httplib2.Http(c, disable_ssl_certificate_validation = True)
    resp, content = h.request(url)
    c.cleanup()        
    return resp, content


try:
    from hashlib import sha1 as _sha, md5 as _md5
except ImportError:
    import sha
    import md5
    _sha = sha.new
    _md5 = md5.new

logger = logging.getLogger(__name__)

re_url_scheme    = re.compile(r'^\w+://')
re_slash         = re.compile(r'[?/:|]+')

def safename(filename):
    """Return a filename suitable for the cache.

    Strips dangerous and common characters to create a filename we
    can use to store the cache in.
    """
    try:
        if re_url_scheme.match(filename):
            if isinstance(filename,str):
                filename = filename.decode('utf-8')
                filename = filename.encode('idna')
            else:
                filename = filename.encode('idna')
    except UnicodeError:
        pass
    if isinstance(filename,unicode):
        filename=filename.encode('utf-8')
    filemd5 = _md5(filename).hexdigest()
    filename = re_url_scheme.sub("", filename)
    filename = re_slash.sub(",", filename)

    # limit length of filename
    if len(filename)>200:
        filename=filename[:200]
    return ",".join((filename, filemd5))

class DistributedFileCache(httplib2.FileCache):
    """Uses a directory as a store for cached files.
    Safe to use in multiple threads or processes running on the same cache.
    path_schema tell how sub directories will be created, in that case 256 directories * 256 directories

    You should use this only for a single file retrieval 

    The process is :
       - get the content of the cache : it will lock it if it did no exist to prevent concurrent download
       - set the content of the cache : it will unlock it
       - cleanup the cache after the operation: unlock every file if some remain
    """
    def __init__(self, cachedir, safe=safename, path_schema = [(0,2),(2,4)], create = False):
        self.cachedir = cachedir
        self.safe = safe
        self.lock = threading.Lock()
        self.exclusive_locks = {}
        self.path_schema = path_schema
        if create:
            self.create_dirs()

    def report_error(self, message, exc_info = True):
        "Report an error"
        logger.error("cache: " + message, exc_info=exc_info)

    def create_dirs_(self, base, path_schema):
        "Initialize the directory tree recursively"        
        try:
            os.mkdir(base)
        except OSError, e:
            if e.errno == errno.EEXIST:
                pass
            else:
                self.report_error("Could not create cache subdirectory %s." % base)
                raise

        if len(path_schema) > 0:
            char_count = path_schema[0][1] - path_schema[0][0]
            count = 16 ** char_count
            for i in range(count):
                key = hex(i)[2:]
                while len(key) < char_count:
                    key = "0" + key

                subpath = os.path.join(base, key)
                self.create_dirs_(subpath, path_schema[1:])
        
    def create_dirs(self):     
        "Initialize the directory, first testing if it was successfully created last time    "
        self.justcreated = True
        path = []
        # Fist check if creation is already done: create the last directory to be created
        for schema in self.path_schema:
            s = "f" * (schema[1] - schema[0])
            path += [s]
        firstpath = os.path.join(self.cachedir, *path)
        if os.path.exists(firstpath):
            # The directory tree already existed
            self.justcreated = False
            return
        else:
            # The tree did not exist, so create it now
            self.create_dirs_(self.cachedir, self.path_schema)            

    def acquire_ex_lock(self, filename):
        "Acquire a lock on a file. It will succeed only if the file did not exist before."
        self.lock.acquire()
        try:
            # Try to create the file, it will raise an exception if the file already exists
            f = os.open(filename, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0644)
            # First mark it, before anything can happen, so we may try to unlock files 
            # that were not really locked,
            # but we don't forget some because of an exception or something
            self.exclusive_locks[filename] = {"fd":f}
            # The file did not exist, so we mark it so anybody trying to 
            fcntl.flock(f, fcntl.LOCK_EX)
        finally:
            self.lock.release()

    def build_path(self, key):
        "Build the path for the given key"
        keymd5 = _md5(key).hexdigest()
        
        parts = []
        for schema in self.path_schema:
            parts += [keymd5[schema[0]:schema[1]]]

        parts += [self.safe(key)]

        return os.path.join(*parts)

    def cache_path(self, key):
        "Build the full path for the given key"
        ret = os.path.join(self.cachedir, self.build_path(key))
        return ret

    def get(self, key, timeout = 600):
        """Get the content of the cache.
        If no content was found, then create a exclusively locked file, so somebody trying just after that will wait until I finish.
        If some content was found, then try to "shared lock" the file, wait for the lock if needed, then return the content of the file"""
        cache_full_path = self.cache_path(key)

        # Try to acquire the lock. It its succeeds, it means that the file did not exist, so that the cache was empty
        try:
            self.acquire_ex_lock(cache_full_path)
            return None
        except OSError, e:
            # If the file did existed it's ok, otherwise we have some issue
            if e.errno != errno.EEXIST:
                self.report_error("Unknown OSError creating cache file %s." % cache_full_path)
                raise
        except:
            self.report_error("Unknown error creating cache file %s." % cache_full_path)
            raise
                
        # The file did exists, but it was locked, as its content is not yet ok
        startTime = time.time()
        sleeping = 1.0
        OK = False
        fd = None
        try:
            fd = os.open(cache_full_path, os.O_RDWR, 0644)
            while (time.time() - startTime) < timeout:
                try:
                    # Try to lock the file, and then unlock it
                    fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                    size = os.fstat(fd).st_size
                    if size == 0:
                        OK = False                        
                    # No unlock as we keep the file open to return it to the caller
#                    fcntl.flock(fd, fcntl.LOCK_UN) 
                    # We succeeded locking the file !
                    else:
                        OK = True
                    break
                except IOError, e:
                    # IF the error is "would block", this is just a sign that the file is still locked by someone else
                    if e.errno == errno.EWOULDBLOCK:
                        time.sleep(sleeping)
                        sleeping *= 2.0
                        if sleeping >= 10.0:
                            sleeping = 10.0
                    else:
                        # If we have another error, we report it
                        self.report_error("Unknown error trying to share lock file %s." % cache_full_path)
                        raise
        except:
            self.report_error("Unknown error waiting for file %s." % cache_full_path)
            raise
        finally:
            if not OK:
                if fd != None:
                    os.close(fd)
                return None
            else:
                return os.fdopen(fd, "r")

    def write_content(self, cache_full_path, fd, content):
        """Write the content to the file"""
        try:
            if isinstance(content, str) or isinstance(content, unicode):
                os.write(fd, content)
                return len(content)
            elif hasattr(content, "read"):
                total = 0
                while True:
                    b = content.read(8192)
                    total += len(b)
                    if len(b) == 0 or b is None:
                        break
                    os.write(fd, b)        
                return total
            else:
                raise Exception("Cache: unsupported type of content variable : %s" % content.__class__.__name__)
        except:
            self.report_error("Unknown error writing file %s." % cache_full_path)
            raise

        return None

    def release_ex_lock(self, cache_full_path, headers = None, content = None):
        """Write to the cache file and release the exclusive lock."""
        if cache_full_path not in self.exclusive_locks and (headers != None or content != None):
            # We did not have locked the file => we unlink it, and try to acquire a exclusive lock on the new one
            try:
                os.unlink(cache_full_path)
            except:
                self.report_error("Unknown error unlinking file %s." % cache_full_path)
                raise
                
            self.acquire_ex_lock(cache_full_path)

        fd = None
        self.lock.acquire()
        try:            

            if cache_full_path in self.exclusive_locks:
                fd = self.exclusive_locks[cache_full_path]["fd"]
                if headers != None:
                    os.write(fd, headers)
                if content != None:
                    self.write_content(cache_full_path, fd, content)

                fcntl.flock(fd, fcntl.LOCK_UN)
                del self.exclusive_locks[cache_full_path]
            else:
                if headers != None or content != None:
                    raise Exception("Invalid setting the cache without locking it first.")
        except:
            self.report_error("Error while writing cache file %s." % cache_full_path)
            raise
            if fd != None:
                os.ftruncate(fd, 0)
            try:
                os.unlink(cache_full_path)
            except:
                pass

            try:
                if fd != None:
                    os.close(fd)
            except:
                pass

            if cache_full_path in self.exclusive_locks:
                del self.exclusive_locks[cache_full_path]
        finally:
            self.lock.release()
        return fd

    def set(self, key, header, content):
        """Write a cache file, and return it as a substitute to the original content."""
        cache_full_path = self.cache_path(key)
        fd = self.release_ex_lock(cache_full_path, header, content)
        if fd == None:
            self.report_error("Error while rereading cache file %s after writing it." % cache_full_path)
            raise
        
        # Rewind the file, and skip the header part
        f = os.fdopen(fd, "r")        
        os.lseek(fd, len(header), os.SEEK_SET)
        return f

    def cleanup(self):
        # Clean all the locks that may remain
        for k in self.exclusive_locks.keys():
            self.release_ex_lock(k)
        
    def delete(self, key):
        cache_full_path = self.cache_path(key)
        try:
            os.unlink(cache_full_path)
            self.release_ex_lock(cache_full_path)        
        except:
            self.report_error("Unknown error removing file %s." % cache_full_path)
            raise
