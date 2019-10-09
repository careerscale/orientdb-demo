package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;


public class EdgeWithNullOutTest {



    private static OrientGraphFactory factory = null;

    @BeforeClass
    public static void setup() {

        factory = new OrientGraphFactory("memory:mlm", "admin", "admin").setupPool(1, 10);

        OrientGraph graph = factory.getNoTx();

        // String creds = "memory:/Users/i500221/git/orientdb-demo/orientdb-java-sample/yourdatabase";
        String creds = "memory:yourdatabase";
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(creds);
        if (!db.exists()) {
            db.create();

        }
        try {
            db.drop();
        } catch (Exception e) {
            System.out.println("test " + e.getMessage());
        }

        // OrientDB odb = new OrientDB(creds, "admin", "admin");

        // OrientDB orientDb = new OrientDB("remote:localhost","root","root",
        // OrientDBConfig.defaultConfig());

        OrientDB orientDb = new OrientDB("plocal: /apps/orientdb/databases", "root", "root", OrientDBConfig.defaultConfig());
        orientDb.createIfNotExists("test", ODatabaseType.PLOCAL);

        ODatabaseDocument session = orientDb.open("test", "admin", "admin");
        // ...
        session.close();
        orientDb.drop("test");
        orientDb.close();



        setupDbSchema(graph);

        // return factory;

    }

    @Test

    public void testMultiEdgeUpdate_single_transsaction() {

        OrientGraph graph = factory.getTx();

        OrientGraph noTxGraph = factory.getNoTx();

        Map<String, Object> params = new HashMap<>();


        OGremlinResultSet result = graph.executeSql("select from PART_OF", params);
        int wCount = 0;
        while (result.iterator().hasNext()) {
            Edge edge = result.iterator().next().getEdge().get();

            graph.executeSql("Update PART_OF set status=1 where out.id=" + edge.outVertex().value("id"), params);

            wCount++;

        }

        // graph.getRawDatabase().command("Delete Vertex Market where name='North America'",
        // params);
        graph.commit();

        graph.close();

        graph = factory.getTx();
        result = graph.executeSql("select from Country", params);
        int rCount = 0;
        while (result.iterator().hasNext()) {
            Vertex v = result.iterator().next().getVertex().get();

            Iterator<Edge> edges = v.edges(Direction.OUT, "PART_OF");

            while (edges.hasNext()) {
                Edge e = edges.next();
                System.out.println(v + " id is " + v.property("id").toString() + " edge id is " + e.property("id").toString()
                        + e.outVertex().property("id").toString());

            }
            // assertEquals(edge.value("status").toString(), "1");
            rCount++;

        }

        // assertEquals(rCount, wCount, "PART_OF edges should be same");
        graph.close();



    }

    private static void setupDbSchema(OrientGraph noTxGraph) {

        noTxGraph.executeSql("ALTER DATABASE DATETIMEFORMAT \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"");
        noTxGraph.executeSql("CREATE CLASS BV EXTENDS V");
        noTxGraph.executeSql("CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE);");
        noTxGraph.executeSql("CREATE CLASS BE EXTENDS E;");
        noTxGraph.executeSql("CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);");



        noTxGraph.executeSql("CREATE SEQUENCE partOfIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE CLASS PART_OF EXTENDS BE;");
        noTxGraph.executeSql("CREATE PROPERTY PART_OF.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql("CREATE PROPERTY PART_OF.id LONG (MANDATORY TRUE, default \"sequence('partOfIdSequence').next()\");");


        noTxGraph.executeSql("CREATE CLASS Market EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE marketIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Market.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql("CREATE PROPERTY Market.id LONG (MANDATORY TRUE, default \"sequence('marketIdSequence').next()\");");
        noTxGraph.executeSql("CREATE PROPERTY Market.code STRING;");

        noTxGraph.executeSql("CREATE INDEX MARKET_ID_INDEX ON Market (id BY VALUE) UNIQUE;");
        noTxGraph.executeSql("CREATE CLASS Country EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE countryIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Country.name STRING (MANDATORY TRUE, MIN 3, MAX 50);");
        noTxGraph.executeSql("CREATE PROPERTY Country.id LONG (MANDATORY TRUE, default \"sequence('countryIdSequence').next()\");");
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

        noTxGraph.executeSql(
                "CREATE Edge PART_OF  from (select from Country where name='Brazil') to (select from Market where name='South America1')  set name='test1'");

        // noTxGraph.executeSql(
        // "CREATE Edge PART_OF from (select from Country where name='India') to (select from Market
        // where name='Asia') set name='test1') ");



    }

}
