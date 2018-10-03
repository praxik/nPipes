import setuptools

with open("README", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="npipes",
    version="0.0.1",
    author="Penn Taylor",
    author_email="rpenn3@gmail.com",
    description="Pipes for distributed computing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/praxik/nPipes",
    packages=setuptools.find_packages(),
    install_requires=[
        "pyyaml >= 3.12",
        "dataclasses >= 0.6"],
    classifiers=[
        "Programming Lanuguage :: Python :: 3.6",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent"],
)
