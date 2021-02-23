import sys
import psycopg2cffi


if sys.version_info[0] == 2:
    # Python 2
    PY2 = True
    PY3 = False
    string_types = basestring,
    text_type = unicode
    from ._lru_cache import lru_cache

else:
    # Python 3
    PY2 = False
    PY3 = True
    string_types = str,
    text_type = str
    from functools import lru_cache


def register():
    sys.modules['psycopg2'] = psycopg2cffi
