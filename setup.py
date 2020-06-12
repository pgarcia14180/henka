import os
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
version = os.environ.get('PYPI_VERSION') or 'latest'

lib_folder = os.path.dirname(os.path.realpath(__file__))
requirementPath = lib_folder + '/requirements.txt'
install_requires = []
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires = f.read().splitlines()

setuptools.setup(
    name="henka",
    version=version,
    author="Pedro Garcia",
    author_email="pgarcia14180@gmail.com",
    description="Henka Lib",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://cencoreg.cencosud.corp/repository/pyprod/",
    packages=setuptools.find_packages(),
    install_requires= install_requires,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
