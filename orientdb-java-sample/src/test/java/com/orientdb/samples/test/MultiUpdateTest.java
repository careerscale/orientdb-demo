package com.orientdb.samples.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MultiUpdateTest {

    private static OrientGraphFactory factory = null;

    @BeforeClass

    public static OrientGraphFactory setup() {

        factory = new OrientGraphFactory("memory:mlm", "admin", "admin").setupPool(1, 10);

        OrientGraph graph = factory.getNoTx();

        setupDbSchema(graph);

        return factory;

    }

    @Test

    public void testMultipleUpdates() throws Exception {

        OrientGraph graph = factory.getTx();

        OrientGraph noTxGraph = factory.getNoTx();

        graph.commit();

        graph.close();

        graph = factory.getTx();

        graph.makeActive();

        Map<String, String> params = new HashMap<>();
        graph.executeSql("Update Country set name='India 1' where name='India'", params);
        graph.executeSql("Update Country set name='USA 1' where name='USA'", params);
        graph.executeSql("Update Country set name='U.K 1' where name='U.K'", params);
        graph.commit();
        graph.close();

        graph = factory.getTx();
        OGremlinResultSet resultSet = graph.executeSql("select from Country where name='India 1'", params);
        Vertex vertex = toVertex(graph, resultSet);
        // resultSet.close();

        assertNotNull(vertex);
        assertEquals(vertex.value("code"), "IN");

        resultSet = graph.executeSql("select from Country where name='USA 1'", params);
        vertex = toVertex(graph, resultSet);
        // resultSet.close();

        assertNotNull(vertex);
        assertEquals(vertex.value("code"), "US");
        resultSet = graph.executeSql("select from Country where name='U.K 1'", params);
        vertex = toVertex(graph, resultSet);

        assertNotNull(vertex);
        assertEquals(vertex.value("code"), "UK");

    }

    public Vertex toVertex(OrientGraph graph, OGremlinResultSet resultSet) {
        if (resultSet.iterator().hasNext()) {
            return resultSet.iterator().next().getVertex().orElse(null);
        }
        return null;

    }

    private static void setupDbSchema(OrientGraph noTxGraph) {

        noTxGraph.executeSql("ALTER DATABASE DATETIMEFORMAT \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"");
        noTxGraph.executeSql("CREATE CLASS BV EXTENDS V");
        noTxGraph.executeSql(
                "CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE);");
        noTxGraph.executeSql("CREATE CLASS BE EXTENDS E;");
        noTxGraph.executeSql(
                "CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);");
        noTxGraph.executeSql("CREATE CLASS Country EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE countryIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Country.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql(
                "CREATE PROPERTY Country.id LONG (MANDATORY TRUE, default \"sequence('countryIdSequence').next()\");");
        noTxGraph.executeSql("CREATE PROPERTY Country.currency STRING (MANDATORY TRUE);");
        noTxGraph.executeSql("CREATE PROPERTY Country.code STRING (MANDATORY TRUE, MIN 2, MAX 3);");
        noTxGraph.executeSql("CREATE INDEX Country_ID_INDEX ON Country (id BY VALUE) UNIQUE;");
        noTxGraph.executeSql("CREATE Vertex Country set name='India', code='IN',currency='INR'");
        noTxGraph.executeSql("CREATE Vertex Country set name='USA', code='US',currency='USD'");
        noTxGraph.executeSql("CREATE Vertex Country set name='U.K', code='UK',currency='GBP'");

    }

}
