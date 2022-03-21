from setuptools import find_packages, setup

TEST_REQUIREMENTS = [
    'pyhamcrest',
    'pytest',
    'tenacity',
]

setup(
    name="ray_beam",
    packages=find_packages(where=".", include="ray_beam_runner*"),
    version="0.0.1",
    author="Ray Team",
    description="An Apache Beam Runner using Ray.",
    long_description="An Apache Beam Runner based on the Ray "
    "distributed computing framework.",
    url="https://github.com/ray-project/ray_beam_runner",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    install_requires=[
        "ray[data]", "apache_beam"
    ],
    extras_require={
        'test': TEST_REQUIREMENTS,
    }
)
