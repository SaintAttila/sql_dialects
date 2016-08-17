This library's purpose is to provide a way to build SQL queries in a 
dialect-agnostic way. The goal is to be able to switch to a new database
which uses a different SQL dialect (e.g. MySQL vs. T-SQL) without 
changing any code, by simply indicating a different data source 
connection string and dialect name in the parameters.
