import unittest2 as unittest


class TestCase(unittest.TestCase):

    def preSetUp(self):
        pass

    def __call__(self, result=None):
        self.preSetUp()
        super(TestCase, self).__call__(result)
