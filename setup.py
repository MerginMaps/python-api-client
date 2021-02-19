# Copyright (C) 2019 Lutra Consulting. All rights reserved.
# Do not distribute without the express permission of the author.

from setuptools import setup, find_packages

setup(
    name='mergin-client',
    version='0.5.8',       # The version is also stored in mergin/version.py
    url='https://github.com/lutraconsulting/mergin-py-client',
    license='MIT',
    author='Lutra Consulting Ltd.',
    author_email='mergin@lutraconsulting.co.uk',
    description='Mergin utils and client',
    long_description='Mergin utils and client',

    packages=find_packages(),

    platforms='any',
    install_requires=[
        'python-dateutil==2.6.0',
        'pygeodiff==0.8.6',
        'pytz==2019.3',
        'click',
    ],

    entry_points={
        'console_scripts': ['mergin=mergin.cli:cli'],
    },

    test_suite='nose.collector',
    tests_require=['nose'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3'
    ],
    package_data={'mergin': ['cert.pem']}
)
