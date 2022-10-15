import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="github_events_analysis",
    version="0.0.1",
    author="Vicente J. Rives Molina",
    author_email="vicente.rives.molina@gmail.com",
    description="Project to get metrics from GitHub events",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vrivesmolina/github_events_analysis",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    include_package_data=True,
    python_requires=">=3.6",
    install_requires=[
        "pyspark",
    ]
)
