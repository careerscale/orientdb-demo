SET ignoreErrors true;
DROP DATABASE remote:/localhost/demo root cloud
SET ignoreErrors false;

## CREATE DATABASE plocal:c:/data/orientdb/databases/demo
## connect plocal:c:/data/orientdb/databases/demo root cloud


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
CREATE INDEX USER_ID_INDEX ON User (id BY VALUE) UNIQUE;


CREATE CLASS Bonus EXTENDS BV;
CREATE SEQUENCE bonusIdSequence TYPE ORDERED;
CREATE PROPERTY Bonus.id LONG (MANDATORY TRUE, default "sequence('bonusIdSequence').next()");
CREATE PROPERTY Bonus.amount DOUBLE (MANDATORY TRUE);
CREATE INDEX BONUS_ID_INDEX ON Bonus (id BY VALUE) UNIQUE;
CREATE CLASS HAS EXTENDS BE;


CREATE CLASS Address EXTENDS BV;
CREATE SEQUENCE addressIdSequence TYPE ORDERED;
CREATE PROPERTY Address.id LONG (MANDATORY TRUE, default "sequence('addressIdSequence').next()");
CREATE PROPERTY Address.street STRING (MANDATORY TRUE);
CREATE INDEX ADDRESS_ID_INDEX ON Address (id BY VALUE) UNIQUE;

CREATE CLASS LIVES_IN EXTENDS BE;


create property LIVES_IN.out LINK User;
create property LIVES_IN.in LINK Address;
create index UNIQUE_LIVES_IN on LIVES_IN(out,in) unique;


CREATE CLASS MANAGES EXTENDS BE;
CREATE CLASS EARNED EXTENDS BE;
CREATE CLASS ROLLS_UP_TO EXTENDS BE;


CREATE VERTEX User SET name='John';
CREATE VERTEX User SET name='Mary';
CREATE VERTEX User SET name='Kevin';
CREATE VERTEX User SET name='Peter';
CREATE VERTEX User SET name='Jenny';

CREATE VERTEX Bonus SET amount=0;
CREATE EDGE EARNED FROM ( SELECT FROM User WHERE name='John') to (select from Bonus order by id desc limit 1);

CREATE VERTEX Bonus SET amount=0;
CREATE EDGE EARNED FROM ( SELECT FROM User WHERE name='Mary') to (select from Bonus order by id desc limit 1);
CREATE EDGE ROLLS_UP_TO FROM (select from Bonus order by id desc limit 1) TO (select from Bonus order by id desc limit 1 skip 1);  


CREATE VERTEX Bonus SET amount=0;
CREATE EDGE EARNED FROM ( SELECT FROM User WHERE name='Peter') to (select from Bonus order by id desc limit 1);
CREATE EDGE ROLLS_UP_TO FROM (select from Bonus order by id desc limit 1) TO (select from Bonus order by id desc limit 1 skip 1);  

CREATE VERTEX Bonus SET amount=0;
CREATE EDGE EARNED FROM ( SELECT FROM User WHERE name='Kevin') to (select from Bonus order by id desc limit 1);
CREATE EDGE ROLLS_UP_TO FROM (select from Bonus order by id desc limit 1) TO (select from Bonus order by id desc limit 1 skip 2);  


CREATE VERTEX Bonus SET amount=0;
CREATE EDGE EARNED FROM ( SELECT FROM User WHERE name='Jenny') to (select from Bonus order by id desc limit 1);
CREATE EDGE ROLLS_UP_TO FROM (select from Bonus order by id desc limit 1) TO (select from Bonus order by id desc limit 1 skip 1);  

 
script sql
LET $a = MATCH {class:User, as:manager, where:(name='John')}, {class:User, as:employee, where:(name='Mary')} RETURN manager, employee;
select expand($a.manager);
select expand($a.employee);
CREATE EDGE MANAGES from ($a.manager) TO ($a.employee);

LET $b = MATCH {class:User, as:manager, where:(name='Mary')}, {class:User, as:employee, where:(name='Peter')} RETURN manager, employee;
CREATE EDGE MANAGES from ($b.manager) TO ($b.employee);
LET $c = MATCH {class:User, as:manager, where:(name='Mary')}, {class:User, as:employee, where:(name='Kevin')} RETURN manager, employee;

CREATE EDGE MANAGES from ($c.manager) TO ($c.employee);
LET $d = MATCH {class:User, as:manager, where:(name='Peter')}, {class:User, as:employee, where:(name='Jenny')} RETURN manager, employee;
CREATE EDGE MANAGES from ($d.manager) TO ($d.employee);

LET $e = MATCH {class:User, as:employee}, {as:employee} <-MANAGES- {class:User, as:manager} RETURN manager, employee;
select $e.employee.id, $e.employee.name, $e.manager.id, $e.manager.name;

end;


SELECT FROM User;
SELECT FROM Bonus;
select from MANAGES;


SELECT FROM V;


## time to build ROLLS_UP_TO relation among bonus vertices.


## Given a member name, I want to add x amount to the bonus of that member, and then to all bonuses of roll up relationships.



