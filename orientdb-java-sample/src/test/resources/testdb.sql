SET ignoreErrors true;
DROP DATABASE remote:localhost/testdb admin admin
SET ignoreErrors false;
CREATE DATABASE remote:localhost/testdb admin admin plocal graph
