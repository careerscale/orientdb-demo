package com.orientdb.samples.test;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

public class DatabaseFactory {
    private static OrientGraph graph;

    private static OrientGraphFactory factory = null;


    public static OrientGraphFactory setup() {
        factory = new OrientGraphFactory("memory:demo", "admin", "admin")

                .setupPool(1, 10);
        OrientGraph graph = factory.getNoTx();
        ODatabaseDocument db = graph.getRawDatabase();
        createDatabase(db);
        return factory;


    }


    public static void createDatabase(ODatabaseDocument db) {

        db.command("CREATE CLASS Person EXTENDS V");
        db.command("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
        db.command("CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default \"sequence('personIdSequence').next()\");");
        db.command("CREATE INDEX Person.id ON Person (id) UNIQUE");

        db.command("ALTER DATABASE DATETIMEFORMAT \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"");
        db.command("CREATE CLASS BV EXTENDS V;");
        db.command(
                "CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        db.command("CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE);");

        db.command("CREATE CLASS BE EXTENDS E;");
        db.command(
                "CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        db.command("CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);");

        db.command("CREATE CLASS User EXTENDS BV;");
        db.command("CREATE SEQUENCE userIdSequence TYPE ORDERED;");

        db.command("CREATE PROPERTY User.id LONG (MANDATORY TRUE, default \"sequence('userIdSequence').next()\");");
        db.command("CREATE PROPERTY User.name STRING (MANDATORY TRUE, MIN 4, MAX 50);");
        db.command("CREATE INDEX USER_ID_INDEX ON User (id BY VALUE) UNIQUE;");


        db.command("CREATE CLASS Bonus EXTENDS BV;");
        db.command("CREATE SEQUENCE bonusIdSequence TYPE ORDERED;");
        db.command("CREATE PROPERTY Bonus.id LONG (MANDATORY TRUE, default \"sequence('bonusIdSequence').next()\");");
        db.command("CREATE PROPERTY Bonus.amount DOUBLE (MANDATORY TRUE);");
        db.command("CREATE INDEX BONUS_ID_INDEX ON Bonus (id BY VALUE) UNIQUE;");
        db.command("CREATE CLASS HAS EXTENDS BE;");


        db.command("CREATE CLASS Address EXTENDS BV;");
        db.command("CREATE SEQUENCE addressIdSequence TYPE ORDERED;");
        db.command(
                "CREATE PROPERTY Address.id LONG (MANDATORY TRUE, default \"sequence('addressIdSequence').next()\");");
        db.command("CREATE PROPERTY Address.street STRING (MANDATORY TRUE)");
        db.command("CREATE INDEX ADDRESS_ID_INDEX ON Address (id BY VALUE) UNIQUE;");

        db.command("CREATE CLASS LIVES_IN EXTENDS BE;");


        db.command("create property LIVES_IN.out LINK User;");
        db.command("create property LIVES_IN.in LINK Address;");
        db.command("create index UNIQUE_LIVES_IN on LIVES_IN(out,in) unique;");


        db.command("CREATE CLASS MANAGES EXTENDS BE;");
        db.command("CREATE CLASS EARNED EXTENDS BE;");
        db.command("CREATE CLASS ROLLS_UP_TO EXTENDS BE;");

    }

}
