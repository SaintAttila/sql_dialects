"""The setup script for the sql_dialects package."""

import os
from setuptools import setup


DIST_FOLDER = r'\\usmmisgroup01001.mm.us.am.ericsson.se\group20ia2\automation\Libraries\DIST\Python'


try:
    # noinspection PyUnresolvedReferences
    import infotags
except ImportError:
    # TODO: Someday, uncomment this and remove the os.system() call below. Before we can do that,
    #       though, we need to make infotags available on the python package index.
    # print("This setup script depends on infotags. Please install infotags using the command, "
    #       "'pip install infotags' and then run this setup script again.")
    # sys.exit(2)
    os.system(r'pip install infotags --find-links ' + DIST_FOLDER)
    import infotags


PACKAGE_NAME = 'sql_dialects'


cwd = os.getcwd()
if os.path.dirname(__file__):
    os.chdir(os.path.dirname(__file__))
try:
    setup(**infotags.get_info(PACKAGE_NAME))
finally:
    os.chdir(cwd)
