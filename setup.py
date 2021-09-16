from setuptools import find_packages, setup

setup(
    name="ray_beam_runner",
    packages=find_packages(where=".", include="ray_beam_runner*"),
    version="0.0.1",
    author="Ray Team",
    description="A Ray runner for Apache Beam",
    long_description="A distributed runner for Apache Beam built on top of "
    "distributed computing framework Ray.",
    url="https://github.com/ray-project/ray_beam_runner",
    install_requires=[
        "ray", "apache_beam"
    ])
