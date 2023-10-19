import setuptools
    
with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name='forloop_modules',
    version='0.4.3',
    author='DovaX',
    author_email='dovax.ai@gmail.com',
    description='This package contains open source modules and integrations within Forloop.ai platform',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/ForloopAI/forloop_modules',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
          ''
     ],
    python_requires='>=3.6',
)
    