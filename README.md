# wh_synchronize

Data synchronization, the synchronization scheme is deliberately made sequentially because there are table relations.
My company currently uses two data warehouses, one for dashboards, analytics, ML purposes and another uses high spec servers and one is only used for backups.
The coding is designed to be flexible so you only need to add table names, primary keys, and omitted columns (optional)
there is an additional primary key in postgre you have to run this script first in SQL.

```
ALTER TABLE table_name ADD CONSTRAINT constraint_name UNIQUE (pkey_column);
```
