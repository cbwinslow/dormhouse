from setuptools import setup, find_packages

setup(
    name="dormouse",
    version="0.3.1",
    author="Sean Fischer",
    author_email="seanwfischer@gmail.com",
    url="https://www.github.com/fischersean/dormouse",
    license="GPLv3",
    description="Database for jabberwocky project",
    python_requires=">=3.7",
    packages=find_packages(
        exclude=["tests", "*.tests", "*.tests.*", "tests.*"]
    ),
    install_requires=[
        "pandas",
        "numpy",
        "pybaseball",
        "sqlalchemy",
        "beautifulsoup4",
    ],
)
