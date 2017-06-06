package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResult;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class EmployeeCascadeTest {
    private static final String BELONGS_TO = "BELONGS_TO";
    private static OrientGraphFactory factory = null;
    public static final String EMPLOYEE = "Employee";
    public static final String NAME = "name";
    public static final String STREET = "street";

    @SuppressWarnings("resource")
    @BeforeTest
    public static OrientGraphFactory setup() {
        // Execute the testdb.sql file in the resource folder
        factory = new OrientGraphFactory("remote:localhost/testdb", "admin", "admin").setupPool(1, 10);
        OrientGraph graph = factory.getNoTx();
        // setupDbSchema(graph);
        return factory;
    }

    @AfterTest
    public static void cleanUp() {

    }

    @Test(priority = 1)
    public void testDbTransactionOperations_Working() {
        // creating countries

        // Block for creating employees
        OrientGraph graph = factory.getTx();
        try {
            graph = factory.getNoTx();
            // Creating employees
            graph.begin();
            Vertex empVertex1 = graph.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(),
                    STREET, "Hacienda");

            OGremlinResultSet vertices = graph.executeSql("select from country");
            Iterator<OGremlinResult> iterator = vertices.iterator();
            while (iterator.hasNext()) {
                empVertex1.addEdge("LIVES_IN", iterator.next().getVertex().get());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != graph && !graph.isClosed()) {
                graph.close();
            }
        }

    }

    @Test
    public void testCreateCountryData() {
        try {
            createCountryData();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createCountryData() {
        OrientGraph graph = null;
        try {
            graph = factory.getTx();
            graph.begin();
            graph.addVertex(T.label, "Country", NAME, "India");
            graph.addVertex(T.label, "Country", NAME, "United States");
            graph.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != graph && !graph.isClosed()) {
                graph.close();
            }
        }
    }

    private static void setupDbSchema(OrientGraph noTxGraph) {
        Map<String, Object> params = new HashMap<String, Object>();
        noTxGraph.executeSql("CREATE CLASS Employee EXTENDS V", params);
        noTxGraph.executeSql("CREATE SEQUENCE employeeIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Employee.id LONG (MANDATORY TRUE, default \"sequence('employeeIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE INDEX Employee.id ON Employee (id) UNIQUE");
        noTxGraph.executeSql("CREATE PROPERTY Employee.name STRING (MANDATORY TRUE, MIN 4, MAX 50);", params);
        noTxGraph.executeSql("CREATE PROPERTY Employee.street STRING (MANDATORY TRUE)", params);

        noTxGraph.executeSql("CREATE CLASS Country EXTENDS V", params);
        noTxGraph.executeSql("CREATE SEQUENCE countryIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Country.id LONG (MANDATORY TRUE, default \"sequence('countryIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE INDEX Country.id ON Country (id) UNIQUE");
        noTxGraph.executeSql("CREATE PROPERTY Country.name STRING (MANDATORY TRUE, MIN 4, MAX 50);", params);
    }

    private static void cleanUpDBSchema(OrientGraph noTxGraph) {
        noTxGraph.executeSql("DELETE class V ;");
    }

}
