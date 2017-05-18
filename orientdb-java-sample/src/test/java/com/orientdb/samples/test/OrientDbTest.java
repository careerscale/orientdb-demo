package com.orientdb.samples.test;

import java.util.List;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

@Test
public class OrientDbTest {
    OrientGraphFactory factory;

    @BeforeClass
    public void setUp() {
        factory = new OrientGraphFactory("remote:localhost/test", "root", "cloud").setupPool(1, 10);
    }



    @BeforeMethod
    private void cleanUp() {
        OrientGraph graph = factory.getTx();
        graph.executeSql("Delete vertex User");
        graph.executeSql("Delete vertex Bonus");
    }

    @Test(priority = 1)
    public void testWithTransactionNormal() {

        OrientGraph graph = factory.getTx();
        try {
            OVertex userVertex = graph.getRawDatabase().newVertex("User");
            userVertex.setProperty("name", "John");
            userVertex.setProperty("status", 1l);
            userVertex.setProperty("id", 1l);
            userVertex.save();

            OVertex bonusVertex = graph.getRawDatabase().newVertex("Bonus");
            bonusVertex.setProperty("name", "Allowance");
            bonusVertex.setProperty("volume", 1000l);
            bonusVertex.setProperty("id", 100);
            bonusVertex.save();

            OVertex bonus1Vertex = graph.getRawDatabase().newVertex("Bonus");
            bonus1Vertex.setProperty("name", "Petrol Allowance");
            bonus1Vertex.setProperty("volume", 200l);
            bonus1Vertex.setProperty("id", 102);
            bonus1Vertex.save();

            userVertex.addEdge(bonusVertex, "HAS").save();
            userVertex.addEdge(bonus1Vertex, "HAS").save();

            graph.commit();

            verifyCount(1);
        } catch (Exception e) {
            graph.rollback();
            e.printStackTrace();

        } finally {
            graph.close();
        }
    }

    @Test(priority = 2)
    public void testWithTransactionRollback() throws Exception {

        OrientGraph graph = factory.getTx();
        try {
            OVertex userVertex = graph.getRawDatabase().newVertex("User");
            userVertex.setProperty("name", "Tony");
            userVertex.setProperty("status", 1l);
            userVertex.setProperty("id", 5l);
            userVertex.save();

            OVertex bonusVertex = graph.getRawDatabase().newVertex("Bonus");
            bonusVertex.setProperty("name", "Allowance");
            bonusVertex.setProperty("volume", 10l);
            bonusVertex.setProperty("id", 104);
            bonusVertex.save();

            OVertex bonus1Vertex = graph.getRawDatabase().newVertex("Bonus");
            bonus1Vertex.setProperty("name", "Petrol Allowance");
            bonus1Vertex.setProperty("id", 105);
            bonus1Vertex.save();

            userVertex.addEdge(bonusVertex, "HAS").save();
            userVertex.addEdge(bonus1Vertex, "HAS").save();

            graph.commit();
        } catch (Exception e) {
            graph.rollback();
            e.printStackTrace();
            // throw new Exception("createUserTransaction ", e);
        } finally {
            graph.close();
        }

        verifyCount(0);


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


    @Test(priority = 3)
    public void getUsers() throws Exception {

        OrientGraph graph = factory.getTx();
        try {
            Iterable<ODocument> userVertexes = graph.getRawDatabase().browseClass("User");

            for (ODocument oDocument : userVertexes) {
                OVertex userVertex = oDocument.asVertex().get();
                System.out.println("UserName------" + userVertex.getProperty("name"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("getUsers ", e);
        } finally {
            graph.close();
        }

    }

    @AfterClass
    public void destroy() {
        factory.close();
    }

}
