from setuptools import setup, find_packages

with open('README.md') as readme_file:
    README = readme_file.read()


setup(
    name="bqcon", # Replace with your own username
    version="0.1.1",
    license = "MIT",
    author="iyappan",
    author_email="iyappan.akp@gmail.com",
    description="Python Package for doing basic operations over google big query ",
    long_description_content_type="text/markdown",
    long_description=README,
    url="https://github.com/iyappan24/bqcon",
    keywords = ['big query wrapper','google big query sdk'],
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.5',
    install_requires=[
         'pandas',
         'google-cloud',
         'google-cloud-bigquery',
        'pandas-gbq'
      ]

)



