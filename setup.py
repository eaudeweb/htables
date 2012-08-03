import distutils.core

distutils.core.setup(
    name='HTables',
    version='0.4b2',
    author='Eau de Web',
    author_email='office@eaudeweb.ro',
    py_modules=['htables'],
    install_requires=[
        'psycopg2',
    ],
)
