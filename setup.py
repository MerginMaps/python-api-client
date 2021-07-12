# Copyright (C) 2019 Lutra Consulting. All rights reserved.
# Do not distribute without the express permission of the author.

from setuptools import setup, find_packages

setup(
    name='mergin-client',
    version='0.6.2',       # The version is also stored in mergin/version.py
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
        'pygeodiff==1.0.1',
        'pytz==2019.3',
        'click',
    ],

    entry_points={
        'console_scripts': ['mergin=mergin.cli:cli'],
    },

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3'
    ],
    package_data={'mergin': ['cert.pem']}
)
