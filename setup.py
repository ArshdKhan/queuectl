from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "click>=8.0.0",
        "flask>=2.0.0",
    ],
    entry_points={
        'console_scripts': [
            'queuectl=queuectl.cli:cli',
        ],
    },
    python_requires='>=3.10',
    description="Production-ready CLI-based background job queue system with worker pools, exponential backoff retries, and Dead Letter Queue",
    author="Arshad Khan",
    author_email="arshadkhan672004@gmail.com",
    url="https://github.com/ArshdKhan/queuectl",
    license="MIT",
    keywords="job-queue cli background-jobs worker-pool dead-letter-queue sqlite",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
    ],
)
