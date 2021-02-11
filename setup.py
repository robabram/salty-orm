#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Copyright (c) 2018 Robert Abram - All Rights Reserved.
#

import os
from distutils.core import setup
from setuptools import find_packages

__VERSION__ = "0.2.10"


def find_package_data_files(dirs):
    paths = []
    for directory in dirs:
        for (path, directories, filenames) in os.walk(directory):
            for filename in filenames:
                paths.append(os.path.join('..', path, filename))
    return paths


def setup_package():

    # Recursively gather all non-python module directories to be included in packaging.
    core_files = find_package_data_files([], )

    setup(name='salty_orm',
        version=__VERSION__,
        description='A Python 3.x ORM implementing a Django ORM object model features.',
        author='Robert Abram',
        author_email='rabram991@gmail.com',
        url='https://github.com/robabram/salty_orm',
        download_url='https://github.com/robabram/salty_orm',
        packages=find_packages(exclude=['tests']),
        package_data={
            'salty_orm': core_files,
        },
        keywords=['orm', 'database'],  # arbitrary keywords
        classifiers=[
            'Development Status :: 4 - Beta',
            'Programming Language :: Python',
            'Environment :: Console',
            'License :: MIT',
            'Operating System :: POSIX :: Linux',
        ],

        # Do not add additional requirements here, add them to requirements.in.
        install_requires=[
            'dateparser',
            'python-dateutil',
            'mysqlclient',
        ],

        entry_points={
          'console_scripts': [], },

        tests_require=[],
     )


if __name__ == "__main__":
    setup_package()
