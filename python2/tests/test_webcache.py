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

    def _test_create_cache(self):
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
 
    def test_get(self):

        url = 'https://ytamex.s3.amazonaws.com/live/assets/4ec56a17-3144-4d9d-be08-704f0ab43299' # BIG
        url = "https://ytamex.s3.amazonaws.com/live/assets/4ec56a17-51f4-4e06-a5fb-704f0ab43299" # SMALL

        urls = [#"https://ytamex.s3.amazonaws.com/live/assets/4ec56a17-fb3c-4aea-ad56-704f0ab43299", # MIDDLE
                #"https://ytamex.s3.amazonaws.com/live/assets/4ec56a17-51f4-4e06-a5fb-704f0ab43299", # SMALL
            "http://assets.stupeflix.com/widgets/customers2/visa/youten/01/%28Footage%29/ASSETS/IMAGES/TrackMattes/square_matte_01.jpg",
#                "http://studio.stupeflix.com/photoservices/picasa/?Signature=c0bee4194d7f1f107651a30c7ce7352d50ce2cd2&photo_id=http%3A%2F%2Fpicasaweb.google.com%2Fdata%2Fentry%2Fapi%2Fuser%2Ffrancois.lagunas%2Falbumid%2F5658835441713341105%2Fphotoid%2F5658835670481409362&method=proxy&proxy=656"  # REDIRECTS
                ]
                
        for url in urls:
            # First open should be slow
            startTime0 = time.time()
            resp, content = cache.urlopen(url, cache_dir, path_schema = [])
            endTime0 = time.time()

            # Then reading should be fast (read from cache file)
            startTime1 = time.time()

#            print content.__class__.__name__
            if isinstance(content, str):
                len0 = len(content)
#                print "len0=", len0
            else:
                len0 = len(content.read())
            endTime1 = time.time()

#            self.assertTrue(endTime1 - startTime1 <= 1.0)

            # Reopening should be very fast
#            print "REOPENING"
            startTime2 = time.time()
            resp, content = cache.urlopen(url, cache_dir, path_schema = [])
            endTime2 = time.time()

#            print endTime0 - startTime0, endTime1 - startTime1, endTime2 - startTime2, len0

#            self.assertTrue(endTime2 - startTime2<= 0.1)

            startTime3 = time.time()
            # And reading should be fast
#            print content.__class__.__name__
            if isinstance(content, str):
                len1 = len(content)
#                print "len1=", len1
            else:
                len1 = len(content.read())

            endTime3 = time.time()

            self.assertTrue(endTime3 - startTime3 <= 1.0)

#            print endTime0 - startTime0, endTime1 - startTime1, endTime2 - startTime2, endTime3 - startTime3, len0, len1

            self.assertEqual(len0, len1)

#            self.assertTrue(False)


 
    def test_get(self):
        url = "http://assets.stupeflix.com/widgets/customers2/visa/youten/01/%28Footage%29/ASSETS/IMAGES/TrackMattes/square_matte_01.jpg"
                
        resp, content = cache.urlopen(url, cache_dir, path_schema = [])

        filename = "assets.stupeflix.com,widgets,customers2,visa,youten,01,%28Footage%29,ASSETS,IMAGES,TrackMattes,square_matte_01.jpg,e15f45b3e62e98ab0de57c46e359bdff"

        f = open(os.path.join(cache_dir, filename))

        print len(content.read())

        resp, content = cache.urlopen(url, cache_dir, path_schema = [])
        print len(content.read())

        f = open(os.path.join(cache_dir, filename))
