from setuptools import find_packages, setup

setup(
    name="ray_beam",
    packages=find_packages(where=".", include="ray_beam_runner*"),
    version="0.0.1",
    author="Ray Team",
    description="An Apache Beam Runner using Ray.",
    long_description="An Apache Beam Runner based on the Ray "
    "distributed computing framework.",
    url="https://github.com/ray-project/ray_beam_runner",
    install_requires=["ray", "apache_beam"],
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
