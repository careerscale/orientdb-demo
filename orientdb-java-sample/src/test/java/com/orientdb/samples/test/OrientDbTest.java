package com.orientdb.samples.test;

import java.util.List;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.orientechnologies.orient.core.record.OVertex;

/**
 * This test shows problem with transaction mgmt in OrientDB. Execute
 * src\test\resources\testSchema.sql first as schema definition
 * 
 * @author hmallepa
 *
 */
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
            System.out.println("should not be printed");
        } catch (Exception e) {
            // graph.rollback();
            e.printStackTrace();
            // throw new Exception("createUserTransaction ", e);
        } finally {
            graph.close();
        }
        System.out.println("should be printed");
        verifyCount(0);


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
