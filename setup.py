# Copyright (C) 2019 Lutra Consulting. All rights reserved.
# Do not distribute without the express permission of the author.

from setuptools import setup, find_packages

setup(
    name='mergin-client',
    version='0.9.1',
    url='https://github.com/MerginMaps/mergin-py-client',
    license='MIT',
    author='Lutra Consulting Ltd.',
    author_email='info@merginmaps.com',
    description='Mergin Maps utils and client',
    long_description='Mergin Maps utils and client',

    packages=find_packages(),

    platforms='any',
    install_requires=[
        'python-dateutil==2.8.2',
        'pygeodiff==2.0.2',
        'pytz==2022.1',
        'click==8.1.3',
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
