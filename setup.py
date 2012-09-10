import os.path
import distutils.core

f = open(os.path.join(os.path.dirname(__file__), 'README.rst'), 'rb')
readme_rst = f.read()
f.close()

summary = ("htables is a database library for storing mapping objects "
           "in a relational database.")

try:
    from htables import __version__
except ImportError:
    __version__ == 'dev'


distutils.core.setup(
    name='htables',
    url='http://packages.python.org/htables/',
    description=summary,
    long_description=readme_rst,
    version=__version__,
    author='Eau de Web',
    author_email='office@eaudeweb.ro',
    py_modules=['htables'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database :: Front-Ends',
    ],
)
