import os.path
import distutils.core

f = open(os.path.join(os.path.dirname(__file__), 'README.rst'), 'rb')
readme_rst = f.read()
f.close()

summary = ("HTables is a database library for storing mapping objects "
           "in a relational database.")

distutils.core.setup(
    name='HTables',
    description=summary,
    long_description=readme_rst,
    version='dev',
    author='Eau de Web',
    author_email='office@eaudeweb.ro',
    py_modules=['htables'],
)
