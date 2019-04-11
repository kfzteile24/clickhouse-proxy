# HTTP proxy for ClickHouse

Requires Python 3.7

Tackles problems with escaping functions from clickhouse-odbc, fixes JOIN conditions

Work in progress

## Precautions

* Jeffrey optimizes queries under the assumption that there are no `NULL` values. Otherwise, Jeffrey would need a brother that would optimize them by using `COALESCE` to account for those NULLs. ClickHouse doesn't support other ways of joining the data how Tableau wants it

* FSM completely ignores quotes (strings), quoted identifiers and comments when parsing queries.

* Jeffrey ignores comments when parsing queries

* Jeffrey doesn't create a proper syntax tree where everything is tokenized. It only looks at keywords being present anywhere in the query (except for strings and identifiers)
