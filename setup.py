from setuptools import setup, find_packages

setup(
    name='htables',
    version='0.1',
    author='Eau de Web',
    author_email='office@eaudeweb.ro',
    packages=find_packages(),
    install_requires=[
        'psycopg2',
    ],
)
