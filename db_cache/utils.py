"""
ScopedSession for using SqlAlchemy in a multithreaded application

:author: Doug Skrypa
"""
import os
import logging
from getpass import getuser
from platform import system

from sqlalchemy.orm import sessionmaker, scoped_session

__all__ = ['ScopedSession', 'validate_or_make_dir', 'get_user_cache_dir']
log = logging.getLogger(__name__)

ON_WINDOWS = system().lower() == 'windows'


class ScopedSession:
    """
    Context manager for working with an SqlAlchemy scoped_session in a multithreaded environment

    :param engine: An `SqlAlchemy Engine
      <http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_
    """
    def __init__(self, engine):
        self._scoped_session = scoped_session(sessionmaker(bind=engine))

    def __enter__(self):
        return self._scoped_session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._scoped_session.remove()


def validate_or_make_dir(dir_path, permissions=None, suppress_perm_change_exc=True) -> str:
    """
    Validate that the given path exists and is a directory.  If it does not exist, then create it and any intermediate
    directories.

    Example value for permissions: 0o1777

    :param str dir_path: The path of a directory that exists or should be created if it doesn't
    :param int permissions: Permissions to set on the directory if it needs to be created (octal notation is suggested)
    :param bool suppress_perm_change_exc: Suppress an OSError if the permission change is unsuccessful (default: suppress/True)
    :return str: The path
    """
    if os.path.exists(dir_path):
        if not os.path.isdir(dir_path):
            raise ValueError('Invalid path - not a directory: {}'.format(dir_path))
    else:
        os.makedirs(dir_path)
        if permissions is not None:
            try:
                os.chmod(dir_path, permissions)
            except OSError as e:
                log.error('Error changing permissions of path {!r} to 0o{:o}: {}'.format(dir_path, permissions, e))
                if not suppress_perm_change_exc:
                    raise e
    return dir_path


def get_user_cache_dir(subdir=None, permissions=None) -> str:
    cache_dir = os.path.join('C:/var/tmp' if ON_WINDOWS else '/var/tmp', getuser(), 'db_cache')
    if subdir:
        cache_dir = os.path.join(cache_dir, subdir)
    validate_or_make_dir(cache_dir, permissions=permissions)
    return cache_dir
