"""
ScopedSession for using SqlAlchemy in a multi-threaded application

:author: Doug Skrypa
"""

import logging
from getpass import getuser
from pathlib import Path
from platform import system
from stat import S_ISDIR
from typing import Union

from sqlalchemy.orm import sessionmaker, scoped_session

__all__ = ['ScopedSession', 'validate_or_make_dir', 'get_user_cache_dir']
log = logging.getLogger(__name__)

ON_WINDOWS = system().lower() == 'windows'

PathLike = Union[Path, str]


class ScopedSession:
    """
    Context manager for working with an SqlAlchemy scoped_session in a multithreaded environment

    :param engine: An `SqlAlchemy Engine
      <http://docs.sqlalchemy.org/en/latest/core/connections.html#sqlalchemy.engine.Engine>`_
    """
    __slots__ = ('_scoped_session',)

    def __init__(self, engine):
        self._scoped_session = scoped_session(sessionmaker(bind=engine))

    def __enter__(self) -> scoped_session:
        return self._scoped_session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._scoped_session.remove()


def validate_or_make_dir(dir_path: PathLike, permissions: int = None, suppress_perm_change_exc: bool = True) -> Path:
    """
    Validate that the given path exists and is a directory.  If it does not exist, then create it and any intermediate
    directories.

    Example value for permissions: 0o1777

    :param dir_path: The path of a directory that exists or should be created if it doesn't
    :param permissions: Permissions to set on the directory if it needs to be created (octal notation is suggested)
    :param suppress_perm_change_exc: Suppress an OSError if the permission change is unsuccessful (default:
      suppress/True)
    :return: The path
    """
    path = Path(dir_path).expanduser()
    try:
        stat_result = path.stat()  # Optimize out 2nd stat() for a chain of .exists() -> .is_dir()
    except (OSError, ValueError):
        pass  # it does not exist
    else:  # it exists
        if S_ISDIR(stat_result.st_mode):
            return path
        else:
            raise ValueError(f'Invalid path - not a directory: {dir_path}')

    path.mkdir(parents=True, exist_ok=True)
    if permissions is not None:
        try:
            path.chmod(permissions)
        except OSError as e:
            log.error(f'Error changing permissions of path {dir_path!r} to 0o{permissions:o}: {e}')
            if not suppress_perm_change_exc:
                raise

    return path


def get_user_cache_dir(subdir: str = None, permissions: int = None) -> Path:
    cache_dir = Path('C:/var/tmp' if ON_WINDOWS else '/var/tmp').joinpath(getuser(), 'db_cache')
    if subdir:
        cache_dir /= subdir
    return validate_or_make_dir(cache_dir, permissions=permissions)
