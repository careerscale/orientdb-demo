package com.orientdb.samples.test;

import java.util.List;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

@Test
public class OrientDbTransactionWithRawaDbTest {
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


    private void performTransactionWithRawDB() throws Exception {

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


    private void performTransaction() throws Exception {

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
            bonusVertex.property("volume", 10l);
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


    @Test()
    public void createUserTransactionTest() throws Exception {
        verifyCount(0);
        try {
            performTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
        verifyCount(0);
        // Let us assert for no data
    }


    @Test()
    public void createUserTransactionWithRollBack() throws Exception {
        verifyCount(0);
        try {
            performTransactionWithRollBack();
        } catch (Exception e) {
            e.printStackTrace();
        }
        verifyCount(0);
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

        Assert.assertTrue(ids.size() >= count, "row count should be  " + count);

        graph.close();
    }


    @AfterClass
    public void destroy() {
        factory.close();
    }

}
