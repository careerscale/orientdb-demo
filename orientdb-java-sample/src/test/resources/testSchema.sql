# Supported data types:
# BOOLEAN	SHORT	DATE	DATETIME	BYTE INTEGER	LONG	STRING	LINK	DECIMAL DOUBLE	FLOAT	BINARY	EMBEDDED	LINKBAG


# Define all types upfront. both nodes(vertices) and relationships (edges) and their attributes so that you can index them

# Let us drop the database if it exists, not sure How to figure it out if it exists. so it is hard drop for now
SET ignoreErrors true;
DROP DATABASE remote:/localhost/test root cloud
SET ignoreErrors false;
CREATE DATABASE remote:/localhost/test root cloud plocal graph

# connect REMOTE:localhost/test root cloud   #https://orientdb.com/docs/2.2/Console-Command-Connect.html
##to get metadata, not so useful  
# select expand(classes) from metadata:schema
ALTER DATABASE DATETIMEFORMAT "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" 

# Master data
CREATE CLASS BV EXTENDS V;
CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE); 
# we can make updatedDate mandatory later.

CREATE CLASS BE EXTENDS E;
CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);
# we can make updatedDate mandatory later.

CREATE CLASS User EXTENDS BV;
CREATE SEQUENCE userIdSequence TYPE ORDERED;
CREATE PROPERTY User.name STRING (MANDATORY TRUE, MIN 3, MAX 50);
CREATE PROPERTY User.id LONG (MANDATORY TRUE, default "sequence('userIdSequence').next()");
CREATE PROPERTY User.status INTEGER (MANDATORY TRUE);
CREATE INDEX USER_ID_INDEX ON User (id BY VALUE) UNIQUE;

CREATE CLASS Bonus EXTENDS BV;
CREATE SEQUENCE bonusIdSequence TYPE ORDERED;
CREATE PROPERTY Bonus.name STRING (MANDATORY TRUE, MIN 3, MAX 50);
CREATE PROPERTY Bonus.id LONG (MANDATORY TRUE, default "sequence('bonusIdSequence').next()");
CREATE PROPERTY Bonus.volume LONG (MANDATORY TRUE);
CREATE INDEX BONUS_ID_INDEX ON Bonus (id BY VALUE) UNIQUE;
CREATE CLASS HAS EXTENDS BE;