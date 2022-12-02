# /usr/bin/env python3
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="archivesspace_jsonmodel_converter",
    version="0.0.1",
    author="Dave and Bobbi Fox",
    author_email="pobocks@gmail.com",
    description="A tool for converting data into ArchivesSpace JSONModel objects.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pobocks/archivesspace_jsonmodel_converter",
    package_dir={"":"src"},
    packages=setuptools.find_namespace_packages(where="src", exclude=("tests")),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={"console_scripts": ["convert=archivesspace_jsonmodel_converter:main"]},
    install_requires=[
        "click>=8.1.3,<9",
        "structlog>=21.1.0,<22",
        "python-json-logger>=2.0.2,<3"
        "pytest>=7.0.1,<8",
        "python-dotenv>0.19.0,<=0.19.1",
        "requests >=2.27.1,<3",
        "psycopg >=3.1.4,<4",
        "typing_extensions==4.4.0"
    ],
    python_requires=">=3.7"
)
