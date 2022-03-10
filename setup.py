import os

from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


long_description = read('README.md') if os.path.isfile("README.md") else ""

setup(
    name='blockchain-spark',
    version='0.3.0b2',
    author='songv',
    author_email='songwei@iftech.io',
    description='Spark extension utils for blockchain',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/tellery/blockchain-spark',
    packages=find_packages(exclude=["test.*", "test"]),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
    keywords='web3, pandas',
    python_requires='>=3.6,<4',
    install_requires=[
        'requests>=2.16.0',
        'eth-utils>=2.0.0'
    ],
    extras_require={
        'pyspark': ["pyspark==3.2.1"],
    },
    project_urls={
        'Bug Reports': 'https://github.com/tellery/blockchain-spark/issues',
        'Source': 'https://github.com/tellery/blockchain-spark',
    },
    tests_require=[
        'nose == 1.3.7',
        'unittest2 >= 1.0.0',
        'web3 == 5.28.0'
    ]
)
