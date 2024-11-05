from setuptools import setup, find_packages

setup(
    name="market_data_pipeline",
    version="0.1.0",
    packages=find_packages(),
    package_dir={"": "src"},
    install_requires=[
        "numpy",
        "pandas",
        "pytest",
        "pytest-asyncio",
        "sortedcontainers",
        "pyyaml",
        "aiohttp",
        "pyarrow"
    ],
)