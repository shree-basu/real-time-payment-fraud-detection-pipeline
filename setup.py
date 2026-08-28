from setuptools import find_packages, setup

setup(
    name="payment-fraud-streaming-reference",
    version="2.0.0",
    packages=find_packages(include=("pipeline*", "simulator*")),
)
