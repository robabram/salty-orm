# Salty-ORM
A Python 3.x ORM implementing a subset of the Django ORM object model features, supports Sqlite and MySql.

The purpose of this ORM is to replicate a good portion of Django's ORM, but not have a Django dependency.

This ORM was written to make it easy to write python command line utilities that perform maintenance and queries on the Django backend database.

The Salty-ORM is very similar to Django's ORM, including supporting queries using a Q object similar to Django's ORM.

**Examples**

Connect to a MySQL Database

`dbconn = MySQLDBConnection()`

`dbconn.db_connect(user='tester', password='****', database='test_db', host='xx.xx.xx.xx', buffered=True)`

`results = TestModel(dbconn).objects.filter(active=1).order_by('a_field')`

Limit Fields Returned

`results = TestModel(dbconn).objects.distinct().values_list('id', 'cert_file').filter(Q('name', QOper.O_LIKE, 'zap*') & Q('name', QOper.O_LIKE, 'zoo*') | Q('name', QOper.O_IS, 'abc') & Q('name', QOper.O_IS, '123')).order_by('modified').all())`

`result = TestModel(dbconn).objects.values_list('id', 'modified', 'name').get(~Q('id', QOper.O_EQUAL, 1) & Q('id', QOper.O_EQUAL, 1))`

Printing SQL Statement

`sql = TestModel(dbconn).objects.filter(active=1).order_by('a_field').to_sql()`

Counts

`total = TestModel(dbconn).objects.count()`

Limit Records Returned
 
`result = TestModel(dbconn).objects.all().limit(10)`

*Note: The Salty-ORM Get() method does not support Django's interpreted name parts. IE: id__eq=1*

See **examples/models.py** for an example of a Salty-ORM model. 

The **utilities/clone_django_models.py** program can be used to scan a Django project and build Salty-ORM compatible models. 

Contributions and questions are welcome.
