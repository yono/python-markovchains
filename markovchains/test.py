#!/usr/bin/env python
# -*- coding:utf-8 -*-
import unittest
from markovchains import *

class TestWord(unittest.TestCase):

    def setUp(self):
        pass

    def test_init(self):
        id = 1
        name = 'name'
        count = 1
        word = Word(id,name,count)
        self.assert_(id,word.id)
        self.assert_(name,word.name)
        self.assert_(count,word.count)

class TestMarkovChains(unittest.TestCase):

    def setUp(self):
        self.dbname = '__test_markovchains__'

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
