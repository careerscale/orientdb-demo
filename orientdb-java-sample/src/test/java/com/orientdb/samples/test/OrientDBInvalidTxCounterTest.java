package com.orientdb.samples.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.shaklee.Address;
import com.shaklee.Bonus;
import com.shaklee.User;

/**
 * Execute the test schema in src/test/resources/test.sql
 * 
 * @author
 *
 */
public class OrientDBInvalidTxCounterTest {

    OrientGraphFactory factory;

    @BeforeClass
    public void setUp() {
        factory = new OrientGraphFactory("remote:localhost/demo", "root", "cloud");
        OrientGraph graph = factory.getTx();
        /*
         * graph.executeSql("Delete vertex User"); graph.executeSql("Delete vertex Bonus");
         * graph.executeSql("Delete vertex Address");
         */
    }



    @Test
    public void createUser_Overtex() {

        OrientGraph graph = factory.getTx();
        try {
            graph.begin();
            OVertex userVertex = graph.getRawDatabase().newVertex("User");
            userVertex.setProperty("name", "Jeff");
            userVertex.setProperty("status", 1l);
            // userVertex.setProperty("id", 1l);
            userVertex.save();

            OVertex bonusVertex = graph.getRawDatabase().newVertex("Bonus");
            bonusVertex.setProperty("amount", 1000);
            // bonusVertex.setProperty("id", 100);
            bonusVertex.save();

            OVertex bonus1Vertex = graph.getRawDatabase().newVertex("Bonus");
            bonus1Vertex.setProperty("amount", 200);
            // bonus1Vertex.setProperty("id", 102);
            bonus1Vertex.save();

            userVertex.addEdge(bonusVertex, "HAS").save();
            userVertex.addEdge(bonus1Vertex, "HAS").save();

            OVertex addressVertex = graph.getRawDatabase().newVertex("Address");
            addressVertex.setProperty("street", "New York");
            addressVertex.save();

            OVertex address1Vertex = graph.getRawDatabase().newVertex("Address");
            address1Vertex.setProperty("street", "Texas");
            address1Vertex.save();

            userVertex.addEdge(addressVertex, "LIVES_IN").save();
            userVertex.addEdge(address1Vertex, "LIVES_IN").save();

            OVertex userVertex2 = graph.getRawDatabase().newVertex("User");
            userVertex2.setProperty("name", "Luca");
            userVertex2.setProperty("status", 1l);
            // userVertex.setProperty("id", 1l);
            userVertex2.save();

            userVertex2.addEdge(addressVertex, "LIVES_IN").save();
            userVertex2.addEdge(address1Vertex, "LIVES_IN").save();

            User user = load(userVertex);
            System.out.println(user);


            graph.commit();
        } catch (Exception e) {
            graph.rollback();
            e.printStackTrace();

        } finally {
            if (!graph.isClosed()) {
                graph.close();
            }
        }
    }

    private User load(OVertex userVertex) {
        User user = User.toUser(userVertex);
        if (null != userVertex) {
            List<Address> address = new ArrayList<>();
            Iterable<OEdge> addressEdges = userVertex.getEdges(ODirection.OUT, User.LIVES_IN);
            for (OEdge edge : addressEdges) {
                address.add(Address.toAddress(edge.getVertex(ODirection.IN)));
            }
            List<Bonus> bonuses = new ArrayList<>();
            Iterable<OEdge> bonusEdges = userVertex.getEdges(ODirection.OUT, User.HAS);
            for (OEdge edge : bonusEdges) {
                bonuses.add(Bonus.toBonus(edge.getVertex(ODirection.IN)));
            }
            user.setBonuses(bonuses);
            user.setAddresses(address);
        }
        return user;
    }


    @Test
    public void createUser_gremlin() {

        OrientGraph graph = factory.getTx();
        try {
            graph.begin();
            Vertex userVertex = graph.addVertex(T.label, "User", "name", "jefff", "status", 1l);

            Vertex bonusVertex = graph.addVertex(T.label, "Bonus", "amount", 1000);
            Vertex bonusVertex2 = graph.addVertex(T.label, "Bonus", "amount", 200);

            userVertex.addEdge("HAS", bonusVertex);
            userVertex.addEdge("HAS", bonusVertex2);


            Vertex addressVertex = graph.addVertex(T.label, "Address", "street", "Homestead Rd");

            Vertex addressVertex2 = graph.addVertex(T.label, "Address", "street", "Saratoga Ave");

            userVertex.addEdge("LIVES_IN", addressVertex);
            userVertex.addEdge("LIVES_IN", addressVertex2);

            Vertex userVertex2 = graph.addVertex(T.label, "User", "name", "Luca", "status", 1l);

            userVertex2.addEdge("LIVES_IN", addressVertex);
            userVertex2.addEdge("LIVES_IN", addressVertex2);


            graph.commit();
        } finally {
            if (!graph.isClosed()) {
                graph.close();
            }
        }
    }



    @Test
    public void createUserTransaction() throws Exception {

        OrientGraph graph = factory.getTx();
        try {
            graph.begin();
            for (int i = 0; i < 15; i++) {
                OVertex userVertex = graph.getRawDatabase().newVertex("User");
                userVertex.setProperty("name", "Tony" + i);
                userVertex.setProperty("status", 1l);
                // userVertex.setProperty("id", 1l);
                userVertex.save();

                OVertex bonusVertex = graph.getRawDatabase().newVertex("Bonus");
                bonusVertex.setProperty("amount", 2000 + i);
                // bonusVertex.setProperty("id", 100);
                bonusVertex.save();

                OVertex bonus1Vertex = graph.getRawDatabase().newVertex("Bonus");
                bonus1Vertex.setProperty("amount", 300 + i);
                // bonus1Vertex.setProperty("id", 102);
                bonus1Vertex.save();

                userVertex.addEdge(bonusVertex, "HAS").save();
                userVertex.addEdge(bonus1Vertex, "HAS").save();

                OVertex addressVertex = graph.getRawDatabase().newVertex("Address");
                addressVertex.setProperty("street", "California" + i);
                addressVertex.save();

                OVertex address1Vertex = graph.getRawDatabase().newVertex("Address");
                address1Vertex.setProperty("street", "Shaklee" + i);
                address1Vertex.save();

                userVertex.addEdge(addressVertex, "LIVES_IN").save();
                userVertex.addEdge(address1Vertex, "LIVES_IN").save();

                User user = load(userVertex);
                System.out.println(user);
            }
            graph.commit();
        } finally {
            if (!graph.isClosed()) {
                graph.close();
            }
        }
    }

}
