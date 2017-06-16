package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MemoryDbDeleteTest {

    private static OrientGraphFactory factory = null;

    @BeforeClass

    public static OrientGraphFactory setup() {

        factory = new OrientGraphFactory("memory:mlm", "admin", "admin").setupPool(1, 10);

        OrientGraph graph = factory.getNoTx();

        setupDbSchema(graph);

        return factory;

    }

    @Test

    public void testCountryDelete() {

        OrientGraph graph = factory.getTx();

        OrientGraph noTxGraph = factory.getNoTx();

        // setupDbSchema(noTxGraph);

        Vertex marketVertex =
                graph.addVertex(T.label, "Market", "name", "North America", "code", "NA", "type", "Countryal");

        Vertex CountryVertex = graph.addVertex(T.label, "Country", "name", "Country1", "code", "XX", "currency", "USD");
        CountryVertex.addEdge("PART_OF", marketVertex);

        graph.commit();

        graph.close();

        graph = factory.getTx();

        graph.makeActive();

        Map<String, String> params = new HashMap<>();
        graph.executeSql("DELETE VERTEX Country where id = 1", params);
        graph.executeSql("DELETE VERTEX Country where id = 1", params);
        graph.getRawDatabase().query("DELETE VERTEX Market where id = ?", 1);

        graph.close();

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
        noTxGraph.executeSql("CREATE CLASS Market EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE marketIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Market.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql(
                "CREATE PROPERTY Market.id LONG (MANDATORY TRUE, default \"sequence('marketIdSequence').next()\");");
        noTxGraph.executeSql("CREATE PROPERTY Market.code STRING (MANDATORY TRUE, MIN 2, MAX 3);");
        noTxGraph.executeSql("CREATE PROPERTY Market.type STRING;");
        noTxGraph.executeSql("CREATE INDEX MARKET_ID_INDEX ON Market (id BY VALUE) UNIQUE;");
        noTxGraph.executeSql("CREATE CLASS Country EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE countryIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Country.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql(
                "CREATE PROPERTY Country.id LONG (MANDATORY TRUE, default \"sequence('countryIdSequence').next()\");");
        noTxGraph.executeSql("CREATE PROPERTY Country.currency STRING (MANDATORY TRUE);");
        noTxGraph.executeSql("CREATE PROPERTY Country.code STRING (MANDATORY TRUE, MIN 2, MAX 3);");
        noTxGraph.executeSql("CREATE INDEX Country_ID_INDEX ON Country (id BY VALUE) UNIQUE;");

    }

}
