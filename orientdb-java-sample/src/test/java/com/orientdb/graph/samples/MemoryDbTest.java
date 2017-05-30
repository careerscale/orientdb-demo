package com.orientdb.graph.samples;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class MemoryDbTest {


    private static OrientGraphFactory factory = null;
    public static final String USER = "User";
    public static final String NAME = "name";
    private static final String AMOUNT = "amount";
    private static final String BONUS = "bonus";

    @BeforeClass
    public static OrientGraphFactory setup() {
        factory = // new OrientGraphFactory("embedded:/graph/", "admin", "admin").setupPool(1, 10);
                new OrientGraphFactory("memory:demo", "admin", "admin").setupPool(1, 10);
        OrientGraph graph = factory.getNoTx();
        setupDbSchema(graph);
        return factory;

    }

    @Test
    public void testDbCrudOperations() {

        OrientGraph graph = factory.getTx();
        OrientGraph noTxGraph = factory.getNoTx();
        setupDbSchema(noTxGraph);

        Vertex vertex = graph.addVertex(T.label, USER, NAME, "first +  last" + new Random().nextDouble());
        vertex.property("address", "test address");

        graph.commit();

        graph.close();
        graph = factory.getTx();
        graph.makeActive();
        OResultSet vertices = graph.executeSql("select from User");


        vertices.vertexStream().forEach(v -> {
            // this works
            String someProperty = v.getProperty("name");

            OrientGraph graph1 = factory.getTx();
            String value = graph1.vertices(v.getRecord().getIdentity()).next().value("name");
            System.out.println("value is " + value);

            try {
                String addressValue = graph1.vertices(v.getRecord().getIdentity()).next().value("address");
            } catch (Exception e) {
                e.printStackTrace();
            }
            graph1.close();

            // graph.vertices(v.)

        });

        graph.close();



    }

    private static void setupDbSchema(OrientGraph noTxGraph) {
        Map<String, Object> params = new HashMap<String, Object>();

        noTxGraph.executeSql("CREATE CLASS Person EXTENDS V", params);
        noTxGraph.executeSql("CREATE SEQUENCE personIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default \"sequence('personIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE INDEX Person.id ON Person (id) UNIQUE");

        noTxGraph.executeSql("ALTER DATABASE DATETIMEFORMAT \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"", params);
        noTxGraph.executeSql("CREATE CLASS BV EXTENDS V;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));",
                params);
        noTxGraph.executeSql("CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE);", params);

        noTxGraph.executeSql("CREATE CLASS BE EXTENDS E;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));",
                params);
        noTxGraph.executeSql("CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);", params);

        noTxGraph.executeSql("CREATE CLASS User EXTENDS BV;", params);
        noTxGraph.executeSql("CREATE SEQUENCE userIdSequence TYPE ORDERED;", params);

        noTxGraph.executeSql(
                "CREATE PROPERTY User.id LONG (MANDATORY TRUE, default \"sequence('userIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE PROPERTY User.name STRING (MANDATORY TRUE, MIN 4, MAX 50);", params);
        noTxGraph.executeSql("CREATE INDEX USER_ID_INDEX ON User (id BY VALUE) UNIQUE;", params);


        noTxGraph.executeSql("CREATE CLASS Bonus EXTENDS BV;", params);
        noTxGraph.executeSql("CREATE SEQUENCE bonusIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Bonus.id LONG (MANDATORY TRUE, default \"sequence('bonusIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE PROPERTY Bonus.amount DOUBLE (MANDATORY TRUE);", params);
        noTxGraph.executeSql("CREATE INDEX BONUS_ID_INDEX ON Bonus (id BY VALUE) UNIQUE;", params);
        noTxGraph.executeSql("CREATE CLASS HAS EXTENDS BE;", params);


        noTxGraph.executeSql("CREATE CLASS Address EXTENDS BV;", params);
        noTxGraph.executeSql("CREATE SEQUENCE addressIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Address.id LONG (MANDATORY TRUE, default \"sequence('addressIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE PROPERTY Address.street STRING (MANDATORY TRUE)", params);
        noTxGraph.executeSql("CREATE INDEX ADDRESS_ID_INDEX ON Address (id BY VALUE) UNIQUE;", params);

        noTxGraph.executeSql("CREATE CLASS LIVES_IN EXTENDS BE;", params);


        noTxGraph.executeSql("create property LIVES_IN.out LINK User;", params);
        noTxGraph.executeSql("create property LIVES_IN.in LINK Address;", params);
        noTxGraph.executeSql("create index UNIQUE_LIVES_IN on LIVES_IN(out,in) unique;", params);


        noTxGraph.executeSql("CREATE CLASS MANAGES EXTENDS BE;", params);
        noTxGraph.executeSql("CREATE CLASS EARNED EXTENDS BE;", params);
        noTxGraph.executeSql("CREATE CLASS ROLLS_UP_TO EXTENDS BE;", params);


    }


}
