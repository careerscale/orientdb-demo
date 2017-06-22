package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class RidBagErrorTest {
    private static final String BELONGS_TO = "BELONGS_TO";
    private static final String MANAGES = "MANAGES";
    private static OrientGraphFactory factory = null;
    public static final String EMPLOYEE = "Employee";
    public static final String NAME = "name";
    public static final String STREET = "street";

    @BeforeTest
    public static OrientGraphFactory setup() {
        // factory = new OrientGraphFactory("memory:trans", "admin", "admin").setupPool(1, 10);
        // make sure demo db exists and credentials are correct
        factory = new OrientGraphFactory("remote:localhost/demo", "root", "cloud").setupPool(1, 10);
        OrientGraph graph = factory.getNoTx();
        // setupDbSchema(graph);
        return factory;
    }

    @AfterTest
    public static void cleanUp() {

    }


    @Test
    public void testforRidBagError() {
        // Block for creating employees
        OrientGraph graph1 = factory.getTx();
        OrientGraph graph2 = factory.getTx();
        try {
            // Creating employees
            Vertex empVertex1 = graph1.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(),
                    STREET, "Street" + new Random().nextDouble());
            Vertex empVertex2 = graph1.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(),
                    STREET, "Street" + new Random().nextDouble());
            Vertex empVertex3 = graph1.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(),
                    STREET, "Street" + new Random().nextDouble());
            Vertex empVertex4 = graph1.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(),
                    STREET, "Street" + new Random().nextDouble());
            Vertex empVertex5 = graph1.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(),
                    STREET, "Street" + new Random().nextDouble());

            graph1.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (null != graph1 && !graph1.isClosed()) {
                graph1.close();
            }
        }



        // Block for testing the created employees count
        int count = 0;
        try {


            for (int i = 1; i < 5; i++) {
                Vertex e1 =
                        graph2.executeSql("select from Employee where id  = ? ", i).iterator().next().getVertex().get();
                Vertex e2 = graph2.executeSql("select from Employee where id  = ? ", i + 1).iterator().next()
                        .getVertex().get();
                e1.addEdge(MANAGES, e2);
            }

            graph2.commit();

        } catch (Exception e) {
            graph2.rollback();
            e.printStackTrace();
            throw e;
        } finally {
            if (null != graph2 && !graph2.isClosed()) {
                graph2.close();
            }
        }


        // Assert.assertEquals(count, 0, "Transaction is not rolleed back, records created");

    }

    private static void setupDbSchema(OrientGraph noTxGraph) {
        Map<String, Object> params = new HashMap<String, Object>();
        try {
            noTxGraph.executeSql("DROP CLASS Employee", params);
            noTxGraph.executeSql("DROP SEQUENCE employeeIdSequence", params);
            noTxGraph.executeSql("DROP Class MANAGES", params);
        } catch (Exception e) {
            e.printStackTrace();
            // thats ok, let us be generous with drops.
        }
        noTxGraph.executeSql("CREATE SEQUENCE employeeIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql("CREATE CLASS Employee EXTENDS V", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Employee.id LONG (MANDATORY TRUE, default \"sequence('employeeIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE INDEX Employee.id ON Employee (id) UNIQUE");
        noTxGraph.executeSql("CREATE PROPERTY Employee.name STRING (MANDATORY TRUE, MIN 4, MAX 50);", params);
        noTxGraph.executeSql("CREATE PROPERTY Employee.street STRING (MANDATORY TRUE)", params);

        noTxGraph.executeSql("CREATE CLASS MANAGES EXTENDS E", params);
    }

    private static void cleanUpDBSchema(OrientGraph noTxGraph) {
        noTxGraph.executeSql("DELETE class V ;");
    }

}
