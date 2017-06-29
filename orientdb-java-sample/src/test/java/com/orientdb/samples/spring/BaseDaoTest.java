package com.orientdb.samples.spring;

import static org.testng.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
@DirtiesContext
@SpringBootTest(classes = Application.class)
@ComponentScan("com.orientdb.*")
public class BaseDaoTest extends AbstractTestNGSpringContextTests {

    private static final Logger LOG = LoggerFactory.getLogger(BaseDaoTest.class);


    @Autowired
    BaseDao baseDao;
    OrientGraph graph = null;

    // @BeforeSuite
    @BeforeClass
    public void setupTestData() throws Exception {

    }

    @Test

    public void testMultiEdgeUpdate_single_transsaction() {

        OrientGraph graph = baseDao.getOrientGraph();

        OrientGraph noTxGraph = baseDao.getNoTxGraph();

        Map<String, Object> params = new HashMap<>();


        OGremlinResultSet result = graph.executeSql("select from PART_OF", params);
        int wCount = 0;
        while (result.iterator().hasNext()) {
            Edge edge = result.iterator().next().getEdge().get();

            graph.executeSql("Update PART_OF set status=1 where out.id=" + edge.outVertex().value("id"), params);

            wCount++;

        }


        graph.commit();

        graph.close();

        result = graph.executeSql("select from PART_OF", params);
        int rCount = 0;
        while (result.iterator().hasNext()) {
            Edge edge = result.iterator().next().getEdge().get();

            System.out.println(edge + " id is " + edge.property("id").toString() + " status is "
                    + edge.property("status").toString());
            assertEquals(edge.value("status").toString(), "1");
            rCount++;

        }

        assertEquals(rCount, wCount, "PART_OF edges should be same");
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
