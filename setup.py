
from pathlib import Path
from setuptools import setup

project_root = Path(__file__).resolve().parent

with project_root.joinpath('readme.rst').open('r', encoding='utf-8') as f:
    long_description = f.read()

about = {}
with project_root.joinpath('db_cache', '__version__.py').open('r', encoding='utf-8') as f:
    exec(f.read(), about)


optional_dependencies = {
    'dev': [                                            # Development env requirements
        'pre-commit',
        'ipython',
    ],
}

setup(
    name=about['__title__'],
    version=about['__version__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    description=about['__description__'],
    long_description=long_description,
    url=about['__url__'],
    project_urls={'Source': about['__url__']},
    packages=['db_cache'],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8'
        'Programming Language :: Python :: 3.9'
        'Programming Language :: Python :: 3.10'
        'Programming Language :: Python :: 3.11'
    ],
    python_requires='>=3.8',
    install_requires=['SQLAlchemy'],
    extras_require=optional_dependencies,
)
