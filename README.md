**This plugin is no longer supported. Please use embulk-input-jdbc's `query` option.**


# SQL input plugin for Embulk

SQL input plugin for Embulk loads records to databases using custom SELECT statement.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **driver_path**: path to the jar file of the JDBC driver (e.g. 'sqlite-jdbc-3.8.7.jar') (string, optional)
- **driver_class**: class name of the JDBC driver (e.g. 'org.sqlite.JDBC') (string, required)
- **url**: URL of the JDBC connection (e.g. 'jdbc:sqlite:mydb.sqlite3') (string, required)
- **user**: database login user name (string, optional)
- **password**: database login password (string, default: optional)
- **schema**: destination schema name (string, default: use the default schema)
- **query**: SELECT statement (string, required)
- **fetch_rows**: number of rows to fetch one time (integer, default: 10000)
- **options**: extra JDBC properties (hash, default: {})

## Example

```yaml
in:
  type: jdbc
  driver_path: /opt/oracle/ojdbc6.jar
  driver_class: oracle.jdbc.driver.OracleDriver
  url: jdbc:oracle:thin:@127.0.0.1:1521:mydb
  user: myuser
  password: "mypassword"
  query: |
    SELECT t1.id, t1.name, t2.id AS t2_id, t2.name AS t2_name
    FROM table1 AS t1
    LEFT JOIN table2 AS t2
      ON t1.id = t2.t1_id
```


## Build

```
$ ./gradlew gem
```
