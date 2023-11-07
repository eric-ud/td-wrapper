from setuptools import setup

setup(
    name="tdwrapper",
    version="0.1",
    description="Overengineered wrapper for the official Teradata SQL Driver for Python.",
    author="Eric Udzhukhu",
    author_email="udzhuhu.eric@gmail.com",
    license="MIT",
    packages=["tdwrapper"],
    install_requires=["pandas==2.0.2", "teradatasql==17.20.0.26"],
)
