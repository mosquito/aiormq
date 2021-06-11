import os
from importlib.machinery import SourceFileLoader

from setuptools import find_packages, setup


module = SourceFileLoader(
    "version", os.path.join("aiormq", "version.py"),
).load_module()


setup(
    name="aiormq",
    version=module.__version__,
    packages=find_packages(exclude=["tests"]),
    license=module.package_license,
    description=module.package_info,
    long_description=open("README.rst").read(),
    url="https://github.com/mosquito/aiormq",
    author=module.__author__,
    author_email=module.team_email,
    install_requires=["pamqp==3.0.1", "yarl"],
    keywords=["rabbitmq", "asyncio", "amqp", "amqp 0.9.1", "driver", "pamqp"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Internet",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Clustering",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Microsoft",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    python_requires=">=3.6",
    package_data={"aiormq": ["py.typed"]},
    extras_require={
        "develop": [
            "aiomisc~=11.0",
            "coverage!=4.3",
            "coveralls",
            "pylava",
            "pytest",
            "pytest-cov",
            "tox>=2.4",
        ],
    },
)
