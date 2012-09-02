import sys, os, re

extensions = ['sphinx.ext.autodoc']
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = u'HTables'
copyright = u'2012, Alex Morega'
exclude_patterns = ['_build']
pygments_style = 'sphinx'
html_theme = 'default'
htmlhelp_basename = 'HTablesdoc'

_repo = os.path.join(os.path.dirname(__file__), '..')
with open(os.path.join(_repo, 'setup.py'), 'rb') as f:
    _line = [l for l in f if 'version' in l][0]
version = release = re.match(r"\s*version\s*=\s*'([^']+)'.*", _line).group(1)

try:
    import htables
except ImportError:
    sys.path.append(_repo)
