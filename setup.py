# Copyright (C) 2019 Lutra Consulting. All rights reserved.
# Do not distribute without the express permission of the author.

from setuptools import setup, find_packages

setup(
    name='mergin-client',
    version='dev',
    url='https://gitlab.cloud.lutraconsulting.co.uk/mergin/py-client',
    license='MIT',
    author='Lutra Consulting Ltd.',
    author_email='mergin@lutraconsulting.co.uk',
    description='Mergin utils and client',
    long_description='Mergin utils and client',

    packages=find_packages(),

    platforms='any',
    install_requires=[],

    test_suite='nose.collector',
    tests_require=['nose'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3'
    ]
)