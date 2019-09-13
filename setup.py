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
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)