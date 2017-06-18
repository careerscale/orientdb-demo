package com.orientdb.samples.test;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MultipleEdgeUpdatesTest {



    private static OrientGraphFactory factory = null;

    @BeforeClass

    public static OrientGraphFactory setup() {

        factory = new OrientGraphFactory("memory:mlm", "admin", "admin").setupPool(1, 10);

        OrientGraph graph = factory.getNoTx();

        setupDbSchema(graph);

        return factory;

    }

    @Test

    public void testMultiEdgeUpdate_single_transsaction() {

        OrientGraph graph = factory.getTx();

        OrientGraph noTxGraph = factory.getNoTx();

        Map<String, Object> params = new HashMap<>();
        graph.executeSql("Update PART_OF set status=1 where out.id=1", params);
        graph.executeSql("Update PART_OF set status=1 where out.id=2", params);
        graph.executeSql("Update PART_OF set status=1 where out.id=3", params);
        graph.executeSql("Update PART_OF set status=1 where out.id=4", params);


        graph.commit();

        graph.close();

        graph = factory.getTx();
        OGremlinResultSet result = graph.executeSql("select from PART_OF", params);
        int count = 0;
        while (result.iterator().hasNext()) {
            Edge edge = result.iterator().next().getEdge().get();
            assertEquals(edge.value("status").toString(), "1");
            System.out.println(edge);
            count++;

        }

        assertEquals(count, 5, "PART_OF edges should be 5 in total");
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



        noTxGraph.executeSql("CREATE SEQUENCE partOfIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE CLASS PART_OF EXTENDS BE;");
        noTxGraph.executeSql("CREATE PROPERTY PART_OF.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql(
                "CREATE PROPERTY PART_OF.id LONG (MANDATORY TRUE, default \"sequence('partOfIdSequence').next()\");");


        noTxGraph.executeSql("CREATE CLASS Market EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE marketIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Market.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql(
                "CREATE PROPERTY Market.id LONG (MANDATORY TRUE, default \"sequence('marketIdSequence').next()\");");
        noTxGraph.executeSql("CREATE PROPERTY Market.code STRING;");

        noTxGraph.executeSql("CREATE INDEX MARKET_ID_INDEX ON Market (id BY VALUE) UNIQUE;");
        noTxGraph.executeSql("CREATE CLASS Country EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE countryIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Country.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql(
                "CREATE PROPERTY Country.id LONG (MANDATORY TRUE, default \"sequence('countryIdSequence').next()\");");
        noTxGraph.executeSql("CREATE PROPERTY Country.code STRING (MANDATORY TRUE, MIN 2, MAX 3);");
        noTxGraph.executeSql("CREATE INDEX Country_ID_INDEX ON Country (id BY VALUE) UNIQUE;");

        noTxGraph.executeSql("CREATE Vertex Market set name='North America', code='NA'");
        noTxGraph.executeSql("CREATE Vertex Market set name='South America', code='SA'");
        noTxGraph.executeSql("CREATE Vertex Market set name='Asia', code='AS'");
        noTxGraph.executeSql("CREATE Vertex Country set name='USA', code='US'");
        noTxGraph.executeSql("CREATE Vertex Country set name='Canada', code='CA'");
        noTxGraph.executeSql("CREATE Vertex Country set name='Mexico', code='MX'");
        noTxGraph.executeSql("CREATE Vertex Country set name='India', code='IN'");
        noTxGraph.executeSql("CREATE Vertex Country set name='Brazil', code='BR'");
        noTxGraph.executeSql(
                "CREATE Edge PART_OF  from (select from Country where name='USA') to (select from Market where name='North America') set name='test1'");
        noTxGraph.executeSql(
                "CREATE Edge PART_OF  from (select from Country where name='Canada') to (select from Market where name='North America') set name='test1'");
        noTxGraph.executeSql(
                "CREATE Edge PART_OF  from (select from Country where name='Mexico') to (select from Market where name='North America ')  set name='test1'");
        noTxGraph.executeSql(
                "CREATE Edge PART_OF  from (select from Country where name='India') to (select from Market where name='Asia')  set name='test1'");
        noTxGraph.executeSql(
                "CREATE Edge PART_OF  from (select from Country where name='Brazil') to (select from Market where name='South America')  set name='test1'");

        // noTxGraph.executeSql(
        // "CREATE Edge PART_OF from (select from Country where name='India') to (select from Market
        // where name='Asia') set name='test1') ");



    }

}
