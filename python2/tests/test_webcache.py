import os
import os.path as op
import shutil
import unittest
import streaming_httplib2.dcache as cache
import time
import subprocess

this_dir = op.dirname(__file__)
data_dir = op.join(this_dir, "data")
tmp_dir = op.join(this_dir, "tmp")
cache_dir = op.join(this_dir, "tmp", "cache")


class TestWebCache(unittest.TestCase):

    def setUp(self):
        self.init()

    def init(self):
        shutil.rmtree(tmp_dir, True)
        os.makedirs(tmp_dir)
        os.makedirs(cache_dir)
    
    def createCache(self):
        self.init()
        self.c = cache.DistributedFileCache(cache_dir, path_schema = [])

    def tearDown(self):
        pass

    def test_create_cache(self):
        self.init()
        self.c = cache.DistributedFileCache(cache_dir, path_schema = [(0,1),(0,1)], create = True)
        
        count = 0
        for root, dirs, files in os.walk(cache_dir):
            count += len(dirs)
            
        self.assertEqual(count, 16 * 16 + 16)

        self.assertTrue(self.c.justcreated)
        self.c = cache.DistributedFileCache(cache_dir, path_schema = [(0,1),(0,1)], create = True)
        self.assertFalse(self.c.justcreated)

    def _test_create_file(self):
        self.createCache()
        t = time.time()
        v = self.c.get("testfile")
        self.assertTrue(time.time() - t < 0.1)

        t = time.time()
        v = self.c.get("testfile", timeout = 2)
        delta = time.time() - t
        print delta
        self.assertTrue(delta < 3.1)
               
    def call(self, prefix, command):
        logfile = op.join(tmp_dir, prefix)

        p = subprocess.Popen(command, shell = True, stderr=open(logfile + ".err", "w"), stdout=open(logfile + ".out", "w"))

        p.wait()
        err = open(logfile + ".err").read()
        out = open(logfile + ".out").read()

        return err, out
 
