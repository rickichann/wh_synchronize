# Data Synchronization

<img width="723" alt="image" src="https://user-images.githubusercontent.com/53082147/231049509-99a42939-9c47-4ab2-b863-966b6e55e76c.png">


Data synchronization, the synchronization scheme is deliberately sequential because there are table relations and because we use odoo to display a lot of data. My company currently uses two data warehouses, one for dashboards, analytics, ML purposes and another uses high spec servers and one is only used for backups.
The coding is designed to be flexible so you only need to add the table name, primary key, and omitted column (optional)

there is an additional primary key in postgre you have to run this script first in SQL:

```
ALTER TABLE table_name ADD CONSTRAINT constraint_name UNIQUE (pkey_column);
```
