#!/usr/bin/env python
#
# test_bug_pep479_namedtuplecursor_iter.py - Test iterating over a
# NamedTupleCursor with no results. PEP 479 in Python 3.7 introduced a
# a breaking change that causes uncaught StopIteration exceptions in
# generators to be treated as a RuntimeError.

import psycopg2cffi as psycopg2
import psycopg2cffi.extras
from psycopg2cffi.tests.psycopg2_tests.testconfig import dsn
from psycopg2cffi.tests.psycopg2_tests.testutils import unittest


class Pep479Tests(unittest.TestCase):

    def setUp(self):
        self.conn = psycopg2.connect(dsn)

    def tearDown(self):
        self.conn.close()

    def test(self):
        curs = self.conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        curs.execute("select 1 as foo where false")
        list(curs)
