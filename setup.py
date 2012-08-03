import distutils.core

distutils.core.setup(
    name='HTables',
    version='dev',
    author='Eau de Web',
    author_email='office@eaudeweb.ro',
    py_modules=['htables'],
    install_requires=[
        'psycopg2',
    ],
)
