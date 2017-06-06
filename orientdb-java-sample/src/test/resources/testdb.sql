SET ignoreErrors true;
DROP DATABASE remote:localhost/testdb root cloud
SET ignoreErrors false;
CREATE DATABASE remote:localhost/testdb root cloud plocal graph
