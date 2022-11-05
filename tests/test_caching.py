#!/usr/bin/env python

import time
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, main

from db_cache import TTLDBCache, DBCache


class TTLDBCacheTest(TestCase):
    def test_memory_init(self):
        cache = DBCache('test', db_path=':memory:')
        self.assertIs(None, cache.cache_dir)
        self.assertEqual('sqlite:///:memory:', str(cache.engine.url))

    def test_clean_old(self):
        with TemporaryDirectory() as tmp_dir:
            tmp_dir = Path(tmp_dir)
            sub_dir = tmp_dir.joinpath('test.dir.db')
            sub_dir.mkdir()
            old_path = tmp_dir.joinpath('test.old.db')
            old_path.touch()
            non_db_path = tmp_dir.joinpath('test.foo.txt')
            non_db_path.touch()
            TTLDBCache('test', cache_dir=tmp_dir, ttl=100)
            self.assertFalse(old_path.exists())
            self.assertTrue(non_db_path.exists())
            self.assertTrue(sub_dir.exists())

    def test_entry_expiry(self):
        with TemporaryDirectory() as tmp_dir:
            db = TTLDBCache('test', cache_dir=tmp_dir, ttl=100)
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


if __name__ == '__main__':
    try:
        main(verbosity=2, exit=False)
    except KeyboardInterrupt:
        print()
