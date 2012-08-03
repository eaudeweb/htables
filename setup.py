import os.path
import distutils.core

f = open(os.path.join(os.path.dirname(__file__), 'README.rst'), 'rb')
description = f.read()
f.close()

distutils.core.setup(
    name='HTables',
    description=description,
    version='0.4b3',
    author='Eau de Web',
    author_email='office@eaudeweb.ro',
    py_modules=['htables'],
)
