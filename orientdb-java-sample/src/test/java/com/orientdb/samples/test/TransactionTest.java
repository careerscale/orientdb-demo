package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class TransactionTest {
    private static OrientGraphFactory factory = null;
    public static final String EMPLOYEE = "Employee";
    public static final String NAME = "name";
    public static final String STREET = "street";

    @BeforeClass
    public static OrientGraphFactory setup() {
        factory = new OrientGraphFactory("memory:transdemo", "admin", "admin").setupPool(1, 10);
        OrientGraph graph = factory.getNoTx();
        setupDbSchema(graph);
        return factory;

    }

    @Test
    public void testDbTransactionOperations() {
        // Block for creating employees
        OrientGraph noTxGraph = null;
        try {
            noTxGraph = factory.getNoTx();
            // Creating employees
            noTxGraph.begin();
            noTxGraph.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(), STREET,
                    "Street" + new Random().nextDouble());

            noTxGraph.addVertex(T.label, EMPLOYEE, NAME, "first +  last" + new Random().nextDouble(), STREET,
                    "Street" + new Random().nextDouble());

            // creating an exception manually
            customException();

            noTxGraph.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != noTxGraph && !noTxGraph.isClosed()) {
                noTxGraph.close();
            }
        }

        // Block for testing the created employees count
        OrientGraph graph = null;
        int count = 0;
        try {
            graph = factory.getTx();
            OResultSet vertices = graph.executeSql("select from Employee");
            while (vertices.hasNext()) {
                vertices.next();
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != graph && !graph.isClosed()) {
                graph.close();
            }
        }

        Assert.assertEquals(count, 0, "Transaction is not rolleed back, records created");

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
    }

    private static void customException() throws Exception {
        throw new Exception("Manually triggering exception to check the rollback");
    }

}
