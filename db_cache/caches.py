"""
Cache classes that store values in an SQLite3 DB, using SQLAlchemy.

:author: Doug Skrypa
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Optional, Mapping

from sqlalchemy import create_engine, MetaData, Table, Column, PickleType, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import NoSuchTableError, OperationalError, NoResultFound

from .utils import ScopedSession, validate_or_make_dir, get_user_cache_dir, PathLike

if TYPE_CHECKING:
    from sqlalchemy.orm import scoped_session

__all__ = ['DBCache', 'DBCacheEntry', 'TTLDBCacheEntry', 'TTLDBCache']
log = logging.getLogger(__name__)

OptStr = Optional[str]
_Path = Optional[PathLike]

Base = declarative_base()
_NotSet = object()


class DBCacheEntry(Base):
    """A key, value pair for use in :class:`DBCache`"""
    __tablename__ = 'cache'

    key = Column(PickleType, primary_key=True, index=True, unique=True)
    value = Column(PickleType)

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}({self.key!r})>'


class TTLDBCacheEntry(Base):
    """A key, value pair for use in :class:`TTLDBCache`"""
    __tablename__ = 'ttl_cache'

    key = Column(PickleType, primary_key=True, index=True, unique=True)
    value = Column(PickleType)
    created = Column(Integer, index=True)

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}({self.key!r}, created={self.created})>'


class DBCache:
    """
    A dictionary-like cache that stores values in an SQLite3 DB.  Old cache files in the cache directory that begin with
    the same ``file_prefix`` and username that have non-matching dates in their filename will be deleted when a cache
    file with a new date is created (unless preserve_old is set to True).

    Based on the args provided and the current user, the final path will be: ``db_dir/file_prefix.user.timestamp.db``

    :param prefix: Prefix for DB cache file names
    :param cache_dir: Directory in which DB cache files should be stored; default: result of
      :func:`get_user_cache_dir<ds_tools.utils.filesystem.get_user_cache_dir>`
    :param cache_subdir: Sub directory within the chosen cache_dir in which the DB should be stored
    :param time_fmt: Datetime format to use for DB cache file names
    :param preserve_old: True to preserve old cache files, False (default) to delete them
    :param db_path: An explicit path to use for the DB instead of a dynamically generated one
    :param entry_cls: The class to use for DB entries
    """
    def __init__(
        self,
        prefix: str,
        cache_dir: PathLike = None,
        cache_subdir: str = None,
        time_fmt: str = '%Y-%m',
        preserve_old: bool = False,
        db_path: PathLike = None,
        entry_cls=DBCacheEntry,
    ):
        engine_url = self._prep_storage(prefix, cache_dir, cache_subdir, time_fmt, preserve_old, db_path)
        self._entry_cls = entry_cls
        self.engine = create_engine(engine_url, echo=False)
        self.meta = MetaData(self.engine)
        try:
            self.table = Table(self._entry_cls.__tablename__, self.meta, autoload=True)
        except NoSuchTableError:
            Base.metadata.create_all(self.engine)
            self.table = Table(self._entry_cls.__tablename__, self.meta, autoload=True)
        self.db_session = ScopedSession(self.engine)
        self._lock = RLock()

    def _prep_storage(
        self, prefix: str, cache_dir: _Path, cache_subdir: OptStr, time_fmt: str, preserve_old: bool, db_path: _Path
    ) -> str:
        if db_path:
            if db_path != ':memory:':
                path = Path(db_path).expanduser().resolve()
                path.parent.mkdir(parents=True, exist_ok=True)
                self.cache_dir = path.parent
            else:
                self.cache_dir = None
            return f'sqlite:///{db_path}'

        if cache_dir:
            cache_dir = Path(cache_dir).expanduser()
            self.cache_dir = validate_or_make_dir((cache_dir / cache_subdir) if cache_subdir else cache_dir)
        else:
            self.cache_dir = get_user_cache_dir(cache_subdir)

        current_db = f'{prefix}.{datetime.now().strftime(time_fmt)}.db'
        if not preserve_old:
            self._cleanup_old_dbs(f'{prefix}.', current_db)

        db_path = self.cache_dir.joinpath(current_db)
        return f'sqlite:///{db_path.as_posix()}'

    def _cleanup_old_dbs(self, db_file_prefix: str, current_db: str):
        for path in self.cache_dir.iterdir():
            if path.name.startswith(db_file_prefix) and path.suffix == '.db' and path.name != current_db:
                try:
                    if path.is_file():
                        log.debug(f'Deleting old cache file: {path.as_posix()}')
                        path.unlink()
                except OSError as e:
                    log.debug(f'{e.__class__.__name__} while deleting old cache file {path.as_posix()}: {e}')

    @classmethod
    def _get_default_key_func(cls):
        return _CacheKey.simple_noself

    def __enter__(self) -> scoped_session:
        self._lock.acquire()
        return self.db_session.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.db_session.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._lock.release()

    def keys(self):
        with self as session:
            for entry in session.query(self._entry_cls):
                yield entry.key

    def values(self):
        with self as session:
            for entry in session.query(self._entry_cls):
                yield entry.value

    def items(self):
        with self as session:
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
        with self._lock:
            try:
                value = self[key]
            except KeyError:
                if default is _NotSet:
                    raise
                return default
            else:
                del self[key]
                return value

    def update(self, data: Mapping):
        with self as session:
            for key, value in data.items():
                entry = self._entry_cls(key=key, value=value)
                session.merge(entry)
            session.commit()

    def __len__(self):
        with self as session:
            return session.query(self._entry_cls).count()

    def __contains__(self, item):
        with self as session:
            return session.query(self._entry_cls).filter_by(key=item).scalar()

    def __getitem__(self, item):
        with self as session:
            try:
                # log.debug('Trying to return {!r}'.format(item))
                return session.query(self._entry_cls).filter_by(key=item).one().value
            except (NoResultFound, OperationalError) as e:
                # log.debug('Did not have cached: {!r}'.format(item))
                raise KeyError(item) from e

    def __setitem__(self, key, value):
        with self as session:
            entry = self._entry_cls(key=key, value=value)
            session.merge(entry)
            session.commit()

    def __delitem__(self, key):
        with self as session:
            try:
                session.query(self._entry_cls).filter_by(key=key).delete()
            except (NoResultFound, OperationalError) as e:
                raise KeyError(key) from e
            else:
                session.commit()


class TTLDBCache(DBCache):
    """
    :param ttl: The time to live, in seconds, for entries in this DBCache
    """

    def __init__(self, *args, ttl: int, **kwargs):
        super().__init__(*args, entry_cls=TTLDBCacheEntry, **kwargs)
        self._ttl = int(ttl)

    def expire(self, expiration: int = None):
        """
        :param expiration: A unix epoch timestamp - items created before this time will be removed from the cache.
          Defaults to the given TTL seconds earlier than the current time.
        """
        with self._lock:
            if expiration is None:
                expiration = int(time.time()) - self._ttl
            with self.db_session as session:
                try:
                    session.query(self._entry_cls).filter(self._entry_cls.created < expiration).delete()
                except (NoResultFound, OperationalError):
                    pass
                else:
                    session.commit()

    def __enter__(self) -> scoped_session:
        self._lock.acquire()
        self.expire()
        return self.db_session.__enter__()

    def update(self, data: Mapping):
        with self as session:
            created = int(time.time())
            for key, value in data.items():
                entry = self._entry_cls(key=key, value=value, created=created)
                session.merge(entry)
            session.commit()

    def __setitem__(self, key, value):
        with self as session:
            entry = self._entry_cls(key=key, value=value, created=int(time.time()))
            session.merge(entry)
            session.commit()


class _CacheKey:
    __slots__ = ('_hash', '_vals')

    def __init__(self, tup):
        self._vals = tup
        self._hash = hash(tup)

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        try:
            return self._vals == other._vals
        except AttributeError:
            return False

    @classmethod
    def _to_tuple(cls, *args, **kwargs):
        return args if not kwargs else args + sum(sorted(kwargs.items()), (cls,))

    @classmethod
    def simple_noself(cls, *args, **kwargs):
        """Return a cache key for the specified hashable arguments, omitting the first positional argument."""
        return cls(cls._to_tuple(*args[1:], **kwargs))
