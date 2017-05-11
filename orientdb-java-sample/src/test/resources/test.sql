SET ignoreErrors true;
DROP DATABASE remote:/localhost/demo root cloud
SET ignoreErrors false;

CREATE DATABASE remote:/localhost/demo root cloud plocal graph
connect remote:localhost/demo root cloud 
ALTER DATABASE DATETIMEFORMAT "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" 
CREATE CLASS BV EXTENDS V;
CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE); 

CREATE CLASS BE EXTENDS E;
CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);

CREATE CLASS User EXTENDS BV;
CREATE SEQUENCE userIdSequence TYPE ORDERED;

CREATE PROPERTY User.id LONG (MANDATORY TRUE, default "sequence('userIdSequence').next()");
CREATE PROPERTY User.name STRING (MANDATORY TRUE, MIN 4, MAX 50);

CREATE VERTEX User SET name='John';
CREATE VERTEX User SET name='Mary';
CREATE VERTEX User SET name='Peter';
CREATE VERTEX User SET name='Jenny';

CREATE CLASS Bonus EXTENDS BV;
CREATE SEQUENCE bonusIdSequence TYPE ORDERED;
CREATE PROPERTY Bonus.id LONG (MANDATORY TRUE, default "sequence('bonusIdSequence').next()");
CREATE PROPERTY Bonus.amount DOUBLE (MANDATORY FALSE);


CREATE CLASS MANAGES EXTENDS BE;
CREATE CLASS EARNED EXTENDS BE;

 
script sql
LET $a = MATCH {class:User, as:manager, where:(name='John')}, {class:User, as:employee, where:(name='Mary')} RETURN manager, employee;
select expand($a.manager);
select expand($a.employee);
CREATE EDGE MANAGES from ($a.manager) TO ($a.employee);
select from MANAGES;
end;
SELECT FROM V;

