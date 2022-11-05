#!/usr/bin/env python

import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import Mock, patch

from db_cache.caches import TTLDBCache, DBCache, _CacheKey
from db_cache.utils import validate_or_make_dir


class TTLDBCacheTest(TestCase):
    def test_invalid_dir(self):
        with TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir).joinpath('foo')
            path.touch()
            with self.assertRaises(ValueError):
                validate_or_make_dir(path)

    def test_default_key_func(self):
        self.assertEqual(_CacheKey.simple_noself, DBCache._get_default_key_func())

    def test_memory_init(self):
        cache = DBCache('test', db_path=':memory:')
        self.assertIs(None, cache.cache_dir)
        self.assertEqual('sqlite:///:memory:', str(cache.engine.url))

    def test_db_session_skips_table_call(self):  # noqa
        with patch('db_cache.caches.Table') as table_mock:
            cache = DBCache('test', db_path=':memory:')
            cache.__dict__['table'] = Mock()
            _ = cache.db_session
            table_mock.assert_not_called()

    def test_error_on_clean_old(self):
        with TemporaryDirectory() as tmp_dir:
            cache = DBCache('test', cache_dir=tmp_dir)
            path_mock = Mock(is_file=Mock(side_effect=OSError))
            path_mock.name = 'foo'
            with patch.object(Path, 'glob', return_value=[path_mock]):
                with self.assertLogs('db_cache.caches', 'DEBUG') as log_ctx:
                    cache._cleanup_old_dbs('foo', 'bar')
                self.assertTrue(any('while deleting old cache file' in line for line in log_ctx.output))

    def test_clean_old(self):
        with TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            sub_dir = tmp_dir.joinpath('test.dir.db')
            sub_dir.mkdir()
            old_path = tmp_dir.joinpath('test.old.db')
            old_path.touch()
            non_db_path = tmp_dir.joinpath('test.foo.txt')
            non_db_path.touch()
            DBCache('test', cache_dir=tmp_dir, preserve_old=True)
            self.assertTrue(old_path.exists())
            DBCache('test', cache_dir=tmp_dir)
            self.assertFalse(old_path.exists())
            self.assertTrue(non_db_path.exists())
            self.assertTrue(sub_dir.exists())

    def test_db_path_dir_created(self):
        with TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            DBCache('test', db_path=tmp_dir.joinpath('foo', 'bar.db'))
            self.assertTrue(tmp_dir.joinpath('foo').exists())

    def test_mapping_methods(self):
        for cache in (DBCache('test', db_path=':memory:'), TTLDBCache('test', ttl=100, db_path=':memory:')):
            with self.subTest(cache=cache):
                cache.update({'a': 1, 'b': 2, 'c': 3})
                with self.assertRaises(KeyError):
                    cache.pop('d')
                self.assertIs(None, cache.get('d'))
                self.assertIs(None, cache.pop('d', None))
                self.assertEqual(4, cache.setdefault('d', 4))
                del cache['d']
                with self.assertRaises(KeyError):
                    del cache['d']
                self.assertEqual(3, cache.get('c'))
                self.assertEqual(3, cache.setdefault('c', 4))
                self.assertEqual(3, cache.pop('c'))
                self.assertEqual({'a': 1, 'b': 2}, dict(cache.items()))
                self.assertEqual({'a', 'b'}, set(cache))
                self.assertEqual({'a', 'b'}, set(cache.keys()))
                self.assertEqual({1, 2}, set(cache.values()))

    def test_entry_expiry(self):
        db = TTLDBCache('test', ttl=100, db_path=':memory:')
        db['a'] = 'test a'
        db['b'] = 'test b'
        with db.db_session as session:
            entry = db._entry_cls(key='c', value='test c', created=int(time.time() - 50))
            session.merge(entry)
            entry = db._entry_cls(key='d', value='test d', created=int(time.time() - 200))
            session.merge(entry)
            session.commit()

        self.assertEqual(len(db), 3)
        self.assertIn('a', db)
        self.assertIn('b', db)
        self.assertIn('c', db)
        self.assertNotIn('d', db)

        db.expire(int(time.time() - 49))
        self.assertEqual(len(db), 2)
        self.assertIn('a', db)
        self.assertIn('b', db)
        self.assertNotIn('c', db)
        self.assertNotIn('d', db)

    def test_cache_key(self):
        ck = _CacheKey.simple_noself(None, 'foo', 'bar')
        self.assertEqual(('foo', 'bar'), ck._vals)
        self.assertEqual(ck._hash, hash(ck))
        self.assertEqual(ck, ck)
        self.assertNotEqual(ck, None)


if __name__ == '__main__':
    try:
        main(verbosity=2, exit=False)
    except KeyboardInterrupt:
        print()
