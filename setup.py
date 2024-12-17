# setup.py
from setuptools import setup, find_packages

setup(
    name='batchprocessor',
    version='0.1',
    packages=find_packages(),
    install_requires=["tqdm"],  # List any dependencies here
    # test_suite='tests',
    author='Duc Huy Nguyen',
    # author_email='your.email@example.com',
    description='Small python module to do batch processing. Use this when you want something lightweight and quick to setup. For usecases when writing batch processing boilerplate code is too repetitive and boring and Apache Beam/Ray is overkill',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/huynd2210/batchprocessor',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
)
