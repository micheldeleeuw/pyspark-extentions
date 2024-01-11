import os

from setuptools import setup, find_packages
# from process import about
# from dunamai import Version

# version = Version.from_git()

setup(
    name=about.__title__,

    # specify version as environment variable, so we can
    # use it for preview builds, otherwise fall back to default
    # version=os.getenv('VERSION', f'{version.base}+{version.distance}.{version.commit}'),
    version='0.0.1'

    packages=find_packages(exclude=['tests', 'tests.*', 'images']),
    include_package_data=True,

    install_requires=[
        'wheel>=0.42.0',
        # 'databricks-cli==0.18.0',
        'pyspark==3.4.1',
        # 'dunamai==1.19.0',
        # 'kafka-python==2.0.2',
    ],
    author=about.__author__,
    author_email=about.__mail__
)