import distutils.core

distutils.core.setup(
    name='htables',
    version='0.2',
    author='Eau de Web',
    author_email='office@eaudeweb.ro',
    py_modules=['htables'],
    install_requires=[
        'psycopg2',
    ],
)
