package com.orientdb.samples.test;

import java.util.List;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.orientechnologies.orient.core.record.OVertex;

/**
 * Execute supporting db script. orientdb-java-sample\src\test\resources\testSchema.sql
 * 
 * 
 *
 */
@Test
public class OrientDbDeleteTransactionTest {

    OrientGraphFactory factory;

    @BeforeClass
    public void setUp() {
        factory = new OrientGraphFactory("remote:localhost/demo", "root", "cloud");
        OrientGraph graph = factory.getTx();
        graph.executeSql("Delete vertex User");
        graph.executeSql("Delete vertex Bonus");
    }



    @Test(priority = 1)
    public void createUser() {

        OrientGraph graph = factory.getTx();
        try {
            graph.begin();
            OVertex userVertex = graph.getRawDatabase().newVertex("User");
            userVertex.setProperty("name", "Jeff");
            userVertex.setProperty("status", 1l);
            // userVertex.setProperty("id", 1l);
            userVertex.save();

            OVertex bonusVertex = graph.getRawDatabase().newVertex("Bonus");
            bonusVertex.setProperty("name", "Allowance");
            bonusVertex.setProperty("amount", 1000);
            // bonusVertex.setProperty("id", 100);
            bonusVertex.save();

            OVertex bonus1Vertex = graph.getRawDatabase().newVertex("Bonus");
            bonus1Vertex.setProperty("name", "Petrol Allowance");
            bonus1Vertex.setProperty("amount", 200);
            // bonus1Vertex.setProperty("id", 102);
            bonus1Vertex.save();

            userVertex.addEdge(bonusVertex, "HAS").save();
            userVertex.addEdge(bonus1Vertex, "HAS").save();

            graph.commit();
        } catch (Exception e) {
            graph.rollback();
            e.printStackTrace();

        } finally {
            graph.close();
        }
    }



    @Test(priority = 2)
    public void createUserTransaction() throws Exception {

        OrientGraph graph = factory.getTx();
        try {
            graph.begin();
            OVertex userVertex = graph.getRawDatabase().newVertex("User");
            userVertex.setProperty("name", "Tony");
            userVertex.setProperty("status", 1l);
            userVertex.setProperty("id", 5l);
            userVertex.save();

            OVertex bonusVertex = graph.getRawDatabase().newVertex("Bonus");
            bonusVertex.setProperty("name", "Allowance");
            bonusVertex.setProperty("amount", 10l);
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
            Assert.assertNotNull(e);
        } finally {
            graph.close();
        }
    }

    @Test(priority = 3)
    public void createMarkUser() {

        OrientGraph graph = factory.getTx();
        try {
            graph.begin();
            OVertex userVertex = graph.getRawDatabase().newVertex("User");
            userVertex.setProperty("name", "Mark");
            userVertex.setProperty("status", 1l);
            // userVertex.setProperty("id", 1l);
            userVertex.save();

            OVertex bonusVertex = graph.getRawDatabase().newVertex("Bonus");
            bonusVertex.setProperty("name", "Medical Allowance");
            bonusVertex.setProperty("amount", 1000);
            // bonusVertex.setProperty("id", 100);
            bonusVertex.save();

            OVertex bonus1Vertex = graph.getRawDatabase().newVertex("Bonus");
            bonus1Vertex.setProperty("name", "Car Allowance");
            bonus1Vertex.setProperty("amount", 200);
            // bonus1Vertex.setProperty("id", 102);
            bonus1Vertex.save();

            userVertex.addEdge(bonusVertex, "HAS").save();
            userVertex.addEdge(bonus1Vertex, "HAS").save();

            graph.commit();
        } catch (Exception e) {
            graph.rollback();
            e.printStackTrace();

        } finally {
            graph.close();
        }
    }


    @Test(priority = 4)
    public void deleteUsers() throws Exception {
        OrientGraph graph = factory.getTx();
        // Verify that there exists 2 records in the db.
        verifyCount(2);
        try {
            // Without Transaction Delete API is working fine
            graph.begin();

            OGremlinResultSet resultSet = graph.executeSql("delete Vertex from User where name= ?", "Jeff");
            resultSet.close();
            graph.commit();
        } catch (Exception e) {
            graph.rollback();
            e.printStackTrace();
            // throw new Exception("deleteUsers ", e);
        } finally {
            graph.close();
        }
        verifyCount(1);
    }


    private void verifyCount(int count) {
        OrientGraph graph = factory.getTx();
        OGremlinResultSet vertices = graph.executeSql("select from User");
        List<Long> ids = Lists.newArrayList();

        vertices.stream().forEach(v -> {
            Long id = v.getProperty("id");
            ids.add(id);
        });
        Assert.assertEquals(ids.size(), count);
        graph.close();
    }

    @AfterClass
    public void destroy() {
        factory.close();
    }

}
