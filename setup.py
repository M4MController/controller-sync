from setuptools import setup, find_packages
from os.path import join, dirname

setup(
    name='m4m_sync',
    version='0.1',
    packages=find_packages(),
    install_requires=open(join(dirname(__file__), 'requirements.txt')).readlines(),
)
