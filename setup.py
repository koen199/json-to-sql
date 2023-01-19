
from setuptools import find_packages, setup

setup(
    name='json_to_sql',
    version='0.0.1',
    author="Van den Maegdenbergh Koen",
    author_email="koen_vdm@hotmail.com",
    description="A fast-api extension for creating standard resource searches",
    packages=find_packages(exclude=["tests"]),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/koen199/fastapi-filter",
    setup_requires=["pytest-runner"],
    tests_require=["pytest", "fastapi", "sqlalchemy", "pydantic"],
    install_requires=["sqlalchemy", "pydantic"]
)
