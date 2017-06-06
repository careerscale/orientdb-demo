package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

@Test
public class OrientDbTransactionTest {
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

    private void performTransactionWithRollBack() throws Exception {

        OrientGraph graph = factory.getTx();
        try {
            Vertex userVertex = graph.addVertex(T.label, "User", "name", "Tony", "status", 1l);
            Vertex user2Vertex = graph.addVertex(T.label, "User", "name", "John", "status", 2l);
            Vertex bonusVertex = graph.addVertex(T.label, "Bonus", "name", "Test bonus", "volume", 100l);
            Vertex bonus2Vertex = graph.addVertex(T.label, "Bonus", "name", "Test bonustest ");

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

    private Map<String, Object> buildUserProperties(String name, Long status) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("class", "User");
        params.put("name", name);
        params.put("status", status);
        return params;
    }

    private Map<String, Object> buildBonusProperties(String name, Long volume) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("class", "Bonus");
        params.put("name", name);
        params.put("volume", volume);

        return params;
    }

    private void performTransaction() throws Exception {

        OrientGraph graph = factory.getNoTx();
        try {
            Vertex userVertex = graph.addVertex(T.label, "User", "name", "Tony", "status", 1l);

            Vertex user2Vertex = graph.addVertex(T.label, "User", "name", "John", "status", 1l);

            Vertex bonusVertex = graph.addVertex(T.label, "Bonus", "name", "Test bonus", "volume", 100l);
            Vertex bonus2Vertex = graph.addVertex(T.label, "Bonus", "name", "Test bonustest ", "volume", 102l);

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


    /**
     * https://github.com/orientechnologies/orientdb/issues/7430 Multiple vertices of same time
     * should be saved in a single transaction, but only one User and one Bonus object are getting
     * saved. leaves partial data back in db.
     * 
     * @throws Exception
     */
    @Test()
    public void createUserTransactionTest() throws Exception {
        verifyCount(0);
        try {
            performTransaction();
        } catch (Exception e) {
            e.printStackTrace();
        }
        verifyCount(2);
        // Let us assert for no data
    }


    /**
     * https://github.com/orientechnologies/orientdb/issues/7429 No data should be saved, but it
     * leaves partial data back in db.
     * 
     * @throws Exception
     */
    @Test()
    public void createUserTransactionWithRollbackTest() throws Exception {
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
