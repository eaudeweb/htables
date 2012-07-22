import unittest2 as unittest
import tempfile
from path import path


class TestCase(unittest.TestCase):

    def preSetUp(self):
        pass

    def __call__(self, result=None):
        self.preSetUp()
        super(TestCase, self).__call__(result)

    def tmpdir(self):
        tmp = path(tempfile.mkdtemp())
        self.addCleanup(tmp.rmtree)
        return tmp
