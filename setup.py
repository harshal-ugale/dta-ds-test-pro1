# A file where all the libraries are kept with their versions.
from setuptools import find_packages, setup

from version import VERSION

package_name = "project_dir"
setup(
    name=package_name,
    version=VERSION,
    description="DESCRIPTION",
    author="AUTHOR",
    install_requires=[
        "json2html==1.3.0",
        "commentjson==0.9.0",
    ],
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    package_data={"": ["config.yaml", "*.sql", "*.json"]},
)
