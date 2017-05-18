package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

@Test
public class OrientDbTransactionWithRawaDbTest {
    OrientGraphFactory factory;

    @BeforeClass
    public void setUp() {
        factory = new OrientGraphFactory("remote:localhost/test", "root", "cloud");
    }


    @BeforeMethod
    private void cleanUp() {
        OrientGraph graph = factory.getTx();
        graph.executeSql("Delete vertex User");
        graph.executeSql("Delete vertex Bonus");
    }


    private void performTransactionWithRawDB() throws Exception {

        OrientGraph graph = factory.getTx();

        try {
            Vertex userVertex = graph.addVertex("User");

            userVertex.property("name", "Tony");
            userVertex.property("status", 1l);
            userVertex.property("id", 5l);


            Vertex bonusVertex = graph.addVertex("Bonus");
            bonusVertex.property("name", "Allowance");
            bonusVertex.property("volume", 10l);
            bonusVertex.property("id", 104);

            // userVertex.addEdge(bonusVertex, "HAS").save();
            // userVertex.addEdge(bonus1Vertex, "HAS").save();

            graph.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("createUserTransaction ", e);
        } finally {
            graph.close();
        }
    }

    private void performTransactionWithRollBack() throws Exception {

        OrientGraph graph = factory.getTx();
        try {
            Vertex userVertex = graph.addVertex("User");
            userVertex.property("name", "Tony");
            userVertex.property("status", 1l);
            userVertex.property("id", 5l);

            Vertex bonusVertex = graph.addVertex("Bonus");
            bonusVertex.property("name", "Allowance");
            bonusVertex.property("volume", 10l);
            bonusVertex.property("id", 104);

            Vertex bonus1Vertex = graph.addVertex("Bonus");
            bonus1Vertex.property("name", "Petrol Allowance");
            bonus1Vertex.property("id", 105);
            userVertex.addEdge("HAS", bonusVertex);
            userVertex.addEdge("HAS", bonus1Vertex);

            graph.commit();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("createUserTransaction ", e);
        } finally {
            graph.close();
        }
    }

    private Map<String, Object> buildUserProperties(String name, Long status) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("name", name);
        params.put("status", status);
        return params;
    }

    private Map<String, Object> buildBonusProperties(String name, Long volume) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("name", name);
        params.put("volume", volume);

        return params;
    }

    private void performTransaction() throws Exception {

        OrientGraph graph = factory.getTx();
        try {

            Vertex userVertex = graph.addVertex("User", buildUserProperties("Tony", 1l));

            Vertex bonusVertex = graph.addVertex("Bonus", buildBonusProperties("TestBonus", 100l));

            Vertex bonus2Vertex = graph.addVertex("Bonus", buildBonusProperties("TestBonus", 100l));
            userVertex.addEdge("HAS", bonusVertex);
            userVertex.addEdge("HAS", bonus2Vertex);

            graph.commit();

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("createUserTransaction ", e);
        } finally {
            graph.close();
        }
    }


    @Test()
    public void createUserTransactionTest() throws Exception {
        // verifyCount(0);
        try {
            performTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // verifyCount(1);
        // Let us assert for no data
    }



    private void verifyCount(int count) {
        OrientGraph graph = factory.getTx();
        OResultSet vertices = graph.executeSql("select from User");

        List<Long> ids = Lists.newArrayList();

        vertices.vertexStream().forEach(v -> {
            Long id = v.getProperty("id");
            ids.add(id);
        });

        Assert.assertEquals(ids.size(), 1);

        graph.close();
    }


    @AfterClass
    public void destroy() {
        factory.close();
    }

}
