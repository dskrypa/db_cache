"""
Cache classes that store values in an SQLite3 DB, using SQLAlchemy.

:author: Doug Skrypa
"""

import logging
import os
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, MetaData, Table, Column, PickleType, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import NoSuchTableError, OperationalError
from wrapt import synchronized

from .utils import ScopedSession, validate_or_make_dir, get_user_cache_dir

__all__ = ['DBCache', 'DBCacheEntry', 'TTLDBCacheEntry', 'TTLDBCache']
log = logging.getLogger(__name__)

Base = declarative_base()
_NotSet = object()


class DBCacheEntry(Base):
    """A key, value pair for use in :class:`DBCache`"""
    __tablename__ = 'cache'

    key = Column(PickleType, primary_key=True, index=True, unique=True)
    value = Column(PickleType)

    def __repr__(self):
        return '<{}({!r})>'.format(type(self).__name__, self.key)


class TTLDBCacheEntry(Base):
    """A key, value pair for use in :class:`TTLDBCache`"""
    __tablename__ = 'ttl_cache'

    key = Column(PickleType, primary_key=True, index=True, unique=True)
    value = Column(PickleType)
    created = Column(Integer, index=True)

    def __repr__(self):
        return '<{}({!r}, created={})>'.format(type(self).__name__, self.key, self.created)


class DBCache:
    """
    A dictionary-like cache that stores values in an SQLite3 DB.  Old cache files in the cache directory that begin with
    the same ``file_prefix`` and username that have non-matching dates in their filename will be deleted when a cache
    file with a new date is created (unless preserve_old is set to True).

    Based on the args provided and the current user, the final path will be: ``db_dir/file_prefix.user.timestamp.db``

    :param str prefix: Prefix for DB cache file names
    :param str|None cache_dir: Directory in which DB cache files should be stored; default: result of
      :func:`get_user_cache_dir<ds_tools.utils.filesystem.get_user_cache_dir>`
    :param str cache_subdir: Sub directory within the chosen cache_dir in which the DB should be stored
    :param str time_fmt: Datetime format to use for DB cache file names
    :param bool preserve_old: True to preserve old cache files, False (default) to delete them
    :param str db_path: An explicit path to use for the DB instead of a dynamically generated one
    :param entry_cls: The class to use for DB entries
    """
    def __init__(
            self, prefix, cache_dir=None, cache_subdir=None, time_fmt='%Y-%m', preserve_old=False, db_path=None,
            entry_cls=DBCacheEntry
    ):
        if not db_path:
            if cache_dir:
                self.cache_dir = os.path.join(cache_dir, cache_subdir) if cache_subdir else cache_dir
                validate_or_make_dir(self.cache_dir)
            else:
                self.cache_dir = get_user_cache_dir(cache_subdir)
            db_file_prefix = '{}.'.format(prefix)
            current_db = '{}{}.db'.format(db_file_prefix, datetime.now().strftime(time_fmt))

            if not preserve_old:
                for fname in os.listdir(self.cache_dir):
                    if fname.startswith(db_file_prefix) and fname.endswith('.db') and fname != current_db:
                        file_path = os.path.join(self.cache_dir, fname)
                        try:
                            if os.path.isfile(file_path):
                                log.debug('Deleting old cache file: {}'.format(file_path))
                                os.remove(file_path)
                        except OSError as e:
                            log.debug('{} while deleting old cache file {}: {}'.format(type(e).__name__, file_path, e))

            db_path = os.path.join(self.cache_dir, current_db)
        else:
            _path = Path(db_path).expanduser().resolve()
            if not _path.exists():
                os.makedirs(_path.parent.as_posix())

        self._entry_cls = entry_cls
        self.engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
        self.meta = MetaData(self.engine)
        try:
            self.table = Table(self._entry_cls.__tablename__, self.meta, autoload=True)
        except NoSuchTableError as e:
            Base.metadata.create_all(self.engine)
            self.table = Table(self._entry_cls.__tablename__, self.meta, autoload=True)
        self.db_session = ScopedSession(self.engine)

    def keys(self):
        with self.db_session as session:
            for entry in session.query(self._entry_cls):
                yield entry.key

    def values(self):
        with self.db_session as session:
            for entry in session.query(self._entry_cls):
                yield entry.value

    def items(self):
        with self.db_session as session:
            for entry in session.query(self._entry_cls):
                yield entry.key, entry.value

    def get(self, item, default=None):
        try:
            return self[item]
        except KeyError:
            return default

    def setdefault(self, key, default):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def pop(self, key, default=_NotSet):
        with synchronized(self):
            try:
                value = self[key]
            except KeyError:
                if default is _NotSet:
                    raise
                return default
            else:
                del self[key]
                return value

    def __len__(self):
        with synchronized(self):
            with self.db_session as session:
                return session.query(self._entry_cls).count()

    def __contains__(self, item):
        with synchronized(self):
            with self.db_session as session:
                return session.query(self._entry_cls).filter_by(key=item).scalar()

    def __getitem__(self, item):
        with synchronized(self):
            with self.db_session as session:
                try:
                    # log.debug('Trying to return {!r}'.format(item))
                    return session.query(self._entry_cls).filter_by(key=item).one().value
                except (NoResultFound, OperationalError) as e:
                    # log.debug('Did not have cached: {!r}'.format(item))
                    raise KeyError(item) from e

    def __setitem__(self, key, value):
        with synchronized(self):
            with self.db_session as session:
                # noinspection PyArgumentList
                entry = self._entry_cls(key=key, value=value)
                session.merge(entry)
                session.commit()

    def __delitem__(self, key):
        with synchronized(self):
            with self.db_session as session:
                try:
                    session.query(self._entry_cls).filter_by(key=key).delete()
                except (NoResultFound, OperationalError) as e:
                    raise KeyError(key) from e
                else:
                    session.commit()


class TTLDBCache(DBCache):
    """
    :param int ttl: The time to live, in seconds, for entries in this DBCache
    """
    def __init__(self, *args, ttl, **kwargs):
        # noinspection PyTypeChecker
        super().__init__(*args, entry_cls=TTLDBCacheEntry, **kwargs)
        self._ttl = int(ttl)

    def expire(self, expiration=None):
        """
        :param int expiration: A unix epoch timestamp - items created before this time will be removed from the cache.
          Defaults to the given TTL seconds earlier than the current time.
        """
        with synchronized(self):
            if expiration is None:
                expiration = int(time.time()) - self._ttl
            with self.db_session as session:
                try:
                    # noinspection PyUnresolvedReferences
                    session.query(self._entry_cls).filter(self._entry_cls.created < expiration).delete()
                except (NoResultFound, OperationalError) as e:
                    pass
                else:
                    session.commit()

    def keys(self):
        with synchronized(self):
            self.expire()
            with self.db_session as session:
                for entry in session.query(self._entry_cls):
                    yield entry.key

    def values(self):
        with synchronized(self):
            self.expire()
            with self.db_session as session:
                for entry in session.query(self._entry_cls):
                    yield entry.value

    def items(self):
        with synchronized(self):
            self.expire()
            with self.db_session as session:
                for entry in session.query(self._entry_cls):
                    yield entry.key, entry.value

    def __len__(self):
        with synchronized(self):
            self.expire()
            return super().__len__()

    def __contains__(self, item):
        with synchronized(self):
            self.expire()
            return super().__contains__(item)

    def __setitem__(self, key, value):
        with synchronized(self):
            self.expire()
            with self.db_session as session:
                # noinspection PyArgumentList
                entry = self._entry_cls(key=key, value=value, created=int(time.time()))
                session.merge(entry)
                session.commit()

    def __getitem__(self, item):
        with synchronized(self):
            self.expire()
            return super().__getitem__(item)
