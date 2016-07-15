from setuptools import setup

setup(
    name='sql_dialects',
    version='0',
    packages=['sql_dialects', 'sql_dialects.dialects'],
    url='',
    license='',
    author='Aaron Hosford',
    author_email='aaron.hosford@ericsson.com',
    description='Dialog-agnostic construction of SQL commands',

    # Registration of built-in plugins. See http://stackoverflow.com/a/9615473/4683578 for
    # an explanation of how plugins work in the general case. Other, separately installable
    # packages can register their own SQL dialects as plugins using this file as an example.
    # They will automatically be made available by name when using this library.
    entry_points={
        'sql_dialects': [
            'T-SQL = sql_dialects.dialects._t_sql:T_SQL',
            # TODO: Add MySQL and SQLite
        ]
    },
)
