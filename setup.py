import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="henka",
    version="0.0.1",
    author="Pedro Garcia",
    author_email="pgarcia14180@gmail.com",
    description="Henka Lib",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://cencoreg.cencosud.corp/repository/pyprod/",
    packages=setuptools.find_packages(),
    install_requires=[
        'elasticsearch==6.3.1',
        'numpy==1.16.3',
        'pandas==0.24.2',
        'protobuf==3.8.0',
        'python-dateutil==2.8.0',
        'pytz==2019.1',
        'six==1.12.0',
        'urllib3==1.25.2',
        'Unidecode==1.0.23',
        'xlrd==1.2.0',
        'et-xmlfile==1.0.1',
        'jdcal==1.4.1',
        'openpyxl==2.6.3',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)