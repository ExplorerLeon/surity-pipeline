
"""
Setup.py module for the workflow's worker utilities.
"""

import setuptools

REQUIRED_PACKAGES = [
    'openpyxl==3.1.2',
]

setuptools.setup(
    name='pipeline',
    version='0.0.1',
    description='Excel etl pipeline workflow package',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
