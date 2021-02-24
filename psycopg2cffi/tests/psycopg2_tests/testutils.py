# testutils.py - utility module for psycopg2 testing.

#
# Copyright (C) 2010-2011 Daniele Varrazzo  <daniele.varrazzo@gmail.com>
#
# psycopg2 is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# In addition, as a special exception, the copyright holders give
# permission to link this program with the OpenSSL library (or with
# modified versions of OpenSSL that use the same license as OpenSSL),
# and distribute linked combinations including the two.
#
# You must obey the GNU Lesser General Public License in all respects for
# all of the code used other than OpenSSL.
#
# psycopg2 is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.


# Use unittest2 if available. Otherwise mock a skip facility with warnings.
import operator
import os
import re
import sys
import types

import six
from functools import wraps
from psycopg2cffi.tests.psycopg2_tests.testconfig import dsn

try:
    import unittest2
    unittest = unittest2
except ImportError:
    import unittest
    unittest2 = None

if hasattr(unittest, 'skipIf'):
    skip = unittest.skip
    skipIf = unittest.skipIf

else:
    import warnings

    def skipIf(cond, msg):
        def skipIf_(f):
            @wraps(f)
            def skipIf__(self):
                if cond:
                    warnings.warn(msg)
                    return
                else:
                    return f(self)
            return skipIf__
        return skipIf_

    def skip(msg):
        return skipIf(True, msg)

    def skipTest(self, msg):
        warnings.warn(msg)
        return

    unittest.TestCase.skipTest = skipTest

# Silence warnings caused by the stubborness of the Python unittest maintainers
# http://bugs.python.org/issue9424
if not hasattr(unittest.TestCase, 'assert_') \
or unittest.TestCase.assert_ is not unittest.TestCase.assertTrue:
    # mavaff...
    unittest.TestCase.assert_ = unittest.TestCase.assertTrue
    unittest.TestCase.failUnless = unittest.TestCase.assertTrue
    unittest.TestCase.assertEquals = unittest.TestCase.assertEqual
    unittest.TestCase.failUnlessEqual = unittest.TestCase.assertEqual


class ConnectingTestCase(unittest.TestCase):
    """A test case providing connections for tests.

    A connection for the test is always available as `self.conn`. Others can be
    created with `self.connect()`. All are closed on tearDown.

    Subclasses needing to customize setUp and tearDown should remember to call
    the base class implementations.
    """
    def setUp(self):
        self._conns = []

    def tearDown(self):
        # close the connections used in the test
        for conn in self._conns:
            if not conn.closed:
                conn.close()

    def connect(self, **kwargs):
        try:
            self._conns
        except AttributeError as e:
            raise AttributeError(
                "%s (did you remember calling ConnectingTestCase.setUp()?)"
                % e)

        import psycopg2cffi as psycopg2
        conn = psycopg2.connect(dsn, **kwargs)
        self._conns.append(conn)
        return conn

    def _get_conn(self):
        if not hasattr(self, '_the_conn'):
            self._the_conn = self.connect()

        return self._the_conn

    def _set_conn(self, conn):
        self._the_conn = conn

    conn = property(_get_conn, _set_conn)

    def assertQuotedEqual(self, first, second, msg=None):
        """Compare two quoted strings disregarding eventual E'' quotes"""
        def f(s):
            if isinstance(s, six.text_type):
                return re.sub(r"\bE'", "'", s)
            elif isinstance(first, bytes):
                return re.sub(br"\bE'", b"'", s)
            else:
                return s

        return self.assertEqual(f(first), f(second), msg)


def decorate_all_tests(obj, *decorators):
    """
    Apply all the *decorators* to all the tests defined in the TestCase *obj*.
    The decorator can also be applied to a decorator: if *obj* is a function,
    return a new decorator which can be applied either to a method or to a
    class, in which case it will decorate all the tests.
    """
    if isinstance(obj, types.FunctionType):
        def decorator(func_or_cls):
            if isinstance(func_or_cls, types.FunctionType):
                return obj(func_or_cls)
            else:
                decorate_all_tests(func_or_cls, obj)
                return func_or_cls

        return decorator

    for n in dir(obj):
        if n.startswith('test'):
            for d in decorators:
                setattr(obj, n, d(getattr(obj, n)))


def skip_if_no_uuid(f):
    """Decorator to skip a test if uuid is not supported by Py/PG."""
    @wraps(f)
    def skip_if_no_uuid_(self):
        try:
            import uuid
        except ImportError:
            return self.skipTest("uuid not available in this Python version")

        try:
            cur = self.conn.cursor()
            cur.execute("select typname from pg_type where typname = 'uuid'")
            has = cur.fetchone()
        finally:
            self.conn.rollback()

        if has:
            return f(self)
        else:
            return self.skipTest("uuid type not available on the server")

    return skip_if_no_uuid_


def skip_if_tpc_disabled(f):
    """Skip a test if the server has tpc support disabled."""
    @wraps(f)
    def skip_if_tpc_disabled_(self):
        from psycopg2cffi import ProgrammingError
        cnn = self.connect()
        cur = cnn.cursor()
        try:
            cur.execute("SHOW max_prepared_transactions;")
        except ProgrammingError:
            return self.skipTest(
                "server too old: two phase transactions not supported.")
        else:
            mtp = int(cur.fetchone()[0])
        cnn.close()

        if not mtp:
            return self.skipTest(
                "server not configured for two phase transactions. "
                "set max_prepared_transactions to > 0 to run the test")
        return f(self)

    return skip_if_tpc_disabled_


def skip_if_no_namedtuple(f):
    @wraps(f)
    def skip_if_no_namedtuple_(self):
        try:
            from collections import namedtuple
        except ImportError:
            return self.skipTest("collections.namedtuple not available")
        else:
            return f(self)

    return skip_if_no_namedtuple_


def skip_if_no_iobase(f):
    """Skip a test if io.TextIOBase is not available."""
    @wraps(f)
    def skip_if_no_iobase_(self):
        try:
            from io import TextIOBase
        except ImportError:
            return self.skipTest("io.TextIOBase not found.")
        else:
            return f(self)

    return skip_if_no_iobase_


def skip_before_postgres(*ver):
    """Skip a test on PostgreSQL before a certain version."""
    ver = ver + (0,) * (3 - len(ver))

    def skip_before_postgres_(f):
        @wraps(f)
        def skip_before_postgres__(self):
            if self.conn.server_version < int("%d%02d%02d" % ver):
                return self.skipTest("skipped because PostgreSQL %s"
                    % self.conn.server_version)
            else:
                return f(self)

        return skip_before_postgres__
    return skip_before_postgres_


def skip_after_postgres(*ver):
    """Skip a test on PostgreSQL after (including) a certain version."""
    ver = ver + (0,) * (3 - len(ver))

    def skip_after_postgres_(f):
        @wraps(f)
        def skip_after_postgres__(self):
            if self.conn.server_version >= int("%d%02d%02d" % ver):
                return self.skipTest("skipped because PostgreSQL %s"
                    % self.conn.server_version)
            else:
                return f(self)

        return skip_after_postgres__
    return skip_after_postgres_


def skip_before_python(*ver):
    """Skip a test on Python before a certain version."""
    def skip_before_python_(f):
        @wraps(f)
        def skip_before_python__(self):
            if sys.version_info[:len(ver)] < ver:
                return self.skipTest("skipped because Python %s"
                    % ".".join(map(str, sys.version_info[:len(ver)])))
            else:
                return f(self)

        return skip_before_python__
    return skip_before_python_


def skip_from_python(*ver):
    """Skip a test on Python after (including) a certain version."""
    def skip_from_python_(f):
        @wraps(f)
        def skip_from_python__(self):
            if sys.version_info[:len(ver)] >= ver:
                return self.skipTest("skipped because Python %s"
                    % ".".join(map(str, sys.version_info[:len(ver)])))
            else:
                return f(self)

        return skip_from_python__
    return skip_from_python_


def skip_if_no_superuser(f):
    """Skip a test if the database user running the test is not a superuser"""
    @wraps(f)
    def skip_if_no_superuser_(self):
        from psycopg2cffi import ProgrammingError
        try:
            return f(self)
        except ProgrammingError as e:
            from psycopg2cffi import errorcodes
            if e.pgcode == errorcodes.INSUFFICIENT_PRIVILEGE:
                self.skipTest("skipped because not superuser")
            else:
                raise

    return skip_if_no_superuser_


def skip_if_green(reason):
    def skip_if_green_(f):
        @wraps(f)
        def skip_if_green__(self):
            from psycopg2cffi.tests.psycopg2_tests.testconfig import green
            if green:
                return self.skipTest(reason)
            else:
                return f(self)

        return skip_if_green__
    return skip_if_green_


skip_copy_if_green = skip_if_green("copy in async mode currently not supported")


def skip_if_no_getrefcount(f):
    @wraps(f)
    def skip_if_no_getrefcount_(self):
        if not hasattr(sys, 'getrefcount'):
            return self.skipTest('skipped, no sys.getrefcount()')
        else:
            return f(self)
    return skip_if_no_getrefcount_


def crdb_version(conn, __crdb_version=None):
    """
    Return the CockroachDB version if that's the db being tested, else None.
    Return the number as an integer similar to PQserverVersion: return
    v20.1.3 as 200103.
    Assume all the connections are on the same db: return a cached result on
    following calls.
    """
    __crdb_version = __crdb_version or list()

    if __crdb_version:
        return __crdb_version[0]

    # Wrapped with try/except to avoid AttributeError as 'Connection' object has no attribute 'info'.
    # Should it be ibe implemented?
    try:
        sver = conn.info.parameter_status("crdb_version")
    except AttributeError:
        sver = None

    if sver is None:
        __crdb_version.append(None)
    else:
        m = re.search(r"\bv(\d+)\.(\d+)\.(\d+)", sver)
        if not m:
            raise ValueError(
                "can't parse CockroachDB version from %s" % sver)

        ver = int(m.group(1)) * 10000 + int(m.group(2)) * 100 + int(m.group(3))
        __crdb_version.append(ver)

    return __crdb_version[0]


def skip_if_crdb(reason, conn=None, version=None):
    """Skip a test or test class if we are testing against CockroachDB.
    Can be used as a decorator for tests function or classes:
        @skip_if_crdb("my reason")
        class SomeUnitTest(UnitTest):
            # ...
    Or as a normal function if the *conn* argument is passed.
    If *version* is specified it should be a string such as ">= 20.1", "< 20",
    "== 20.1.3": the test will be skipped only if the version matches.
    """
    if not isinstance(reason, six.string_types):
        raise TypeError("reason should be a string, got %r instead" % reason)

    if conn is not None:
        ver = crdb_version(conn)
        if ver is not None and _crdb_match_version(ver, version):
            if reason in crdb_reasons:
                reason = (
                    "%s (https://github.com/cockroachdb/cockroach/issues/%s)"
                    % (reason, crdb_reasons[reason]))
            raise unittest.SkipTest(
                "not supported on CockroachDB %s: %s" % (ver, reason))

    @decorate_all_tests
    def skip_if_crdb_(f):
        @wraps(f)
        def skip_if_crdb__(self, *args, **kwargs):
            skip_if_crdb(reason, conn=self.connect(), version=version)
            return f(self, *args, **kwargs)

        return skip_if_crdb__

    return skip_if_crdb_


crdb_reasons = {
    "2-phase commit": 22329,
    "backend pid": 35897,
    "cancel": 41335,
    "cast adds tz": 51692,
    "cidr": 18846,
    "composite": 27792,
    "copy": 41608,
    "deferrable": 48307,
    "encoding": 35882,
    "hstore": 41284,
    "infinity date": 41564,
    "interval style": 35807,
    "large objects": 243,
    "named cursor": 41412,
    "nested array": 32552,
    "notify": 41522,
    "range": 41282,
    "stored procedure": 1751,
}


def _crdb_match_version(version, pattern):
    if pattern is None:
        return True

    m = re.match(r'^(>|>=|<|<=|==|!=)\s*(\d+)(?:\.(\d+))?(?:\.(\d+))?$', pattern)
    if m is None:
        raise ValueError(
            "bad crdb version pattern %r: should be 'OP MAJOR[.MINOR[.BUGFIX]]'"
            % pattern)

    ops = {'>': 'gt', '>=': 'ge', '<': 'lt', '<=': 'le', '==': 'eq', '!=': 'ne'}
    op = getattr(operator, ops[m.group(1)])
    ref = int(m.group(2)) * 10000 + int(m.group(3) or 0) * 100 + int(m.group(4) or 0)
    return op(version, ref)


def script_to_py3(script):
    """Convert a script to Python3 syntax if required."""
    if sys.version_info[0] < 3:
        return script

    import tempfile
    f = tempfile.NamedTemporaryFile(suffix=".py", delete=False)
    f.write(script.encode())
    f.flush()
    filename = f.name
    f.close()

    # 2to3 is way too chatty
    import logging
    logging.basicConfig(filename=os.devnull)

    from lib2to3.main import main
    if main("lib2to3.fixes", ['--no-diffs', '-w', '-n', filename]):
        raise Exception('py3 conversion failed')

    f2 = open(filename)
    try:
        return f2.read()
    finally:
        f2.close()
        os.remove(filename)


def _u(s):
    assert isinstance(s, six.binary_type)
    return s.decode('utf-8')
