package com.orientdb.samples.test;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

@Test
public class GraphApiIdBugTest {


    public static final String CLASS_PREFIX = "class:";
    public static final String USER = "User";
    public static final String NAME = "name";


    @Test
    public void testIdBug() {
        OrientGraph graph = new OrientGraphFactory("remote:localhost/demo", "root", "cloud").setupPool(1, 10).getTx();
        // graph.addVertex(id, prop)

        for (int i = 0; i < 10; i++) {
            graph.addVertex(CLASS_PREFIX + USER, createProperties());

        }
        /*
         * graph.addVertex(CLASS_PREFIX + USER, createProperties()); graph.addVertex(CLASS_PREFIX +
         * USER, createProperties()); graph.addVertex(CLASS_PREFIX + USER, createProperties());
         */
        graph.commit();

    }

    private Map<String, Object> createProperties() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(NAME, "first +  last");

        return props;
    }

}
