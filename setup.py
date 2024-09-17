import setuptools

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

with open('pyproject.toml', 'r') as f:
    project_toml = tomllib.loads(f.read())

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name=project_toml['project']['name'],
    version=project_toml['project']['version'],
    author=project_toml['project']['authors'][0]['name'],
    author_email=project_toml['project']['authors'][0]['email'],
    description=project_toml['project']['description'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=project_toml['project']['urls']['Repository'],
    packages=setuptools.find_packages(),
    classifiers=project_toml['project']['classifiers'],
    install_requires=project_toml['project']['dependencies'],
    python_requires=project_toml['project']['requires-python'],
)