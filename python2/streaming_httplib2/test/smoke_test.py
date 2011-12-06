import os
import unittest

import stupeflix.webcache.httplib2_patched as httplib2

from stupeflix.webcache.httplib2_patched.test import miniserver


class HttpSmokeTest(unittest.TestCase):
    def setUp(self):
        self.httpd, self.port = miniserver.start_server(
            miniserver.ThisDirHandler)

    def tearDown(self):
        self.httpd.shutdown()

    def testGetFile(self):
        client = httplib2.Http()
        src = 'miniserver.py'
        url = 'http://localhost:%d/%s' % (self.port, src)
        response, body = client.request(url)

        body = body.read()
        
        self.assertEqual(response.status, 200)
        self.assertEqual(body, open(os.path.join(miniserver.HERE, src)).read())
