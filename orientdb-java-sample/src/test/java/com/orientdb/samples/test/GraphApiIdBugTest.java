package com.orientdb.samples.test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;

@Test
public class GraphApiIdBugTest {


    public static final String CLASS_PREFIX = "class:";
    public static final String USER = "User";
    public static final String NAME = "name";
    private static final String AMOUNT = "amount";
    private static final String BONUS = "bonus";
    private OrientGraphFactory factory = null;

    static {

        try {
            OServer server = OServerMain.create();
            server.startup("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" + "<orient-server>"
                    + "<network>" + "<protocols>"
                    + "<protocol name=\"binary\" implementation=\"com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary\"/>"
                    + "<protocol name=\"http\" implementation=\"com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb\"/>"
                    + "</protocols>" + "<listeners>"
                    + "<listener ip-address=\"0.0.0.0\" port-range=\"2424-2430\" protocol=\"binary\"/>"
                    + "<listener ip-address=\"0.0.0.0\" port-range=\"2480-2490\" protocol=\"http\"/>" + "</listeners>"
                    + "</network>" + "<users>" + "<user name=\"root\" password=\"ThisIsA_TEST\" resources=\"*\"/>"
                    + "</users>" + "<properties>"
                    + "<entry name=\"orientdb.www.path\" value=\"C:/git_repo/orientdb/distribution/target/orientdb-community-3.0.0-SNAPSHOT.dir/orientdb-community-3.0.0-SNAPSHOT/www/\"/>"
                    + "<entry name=\"orientdb.config.file\" value=\"C:/git_repo/orientdb/distribution/target/orientdb-community-3.0.0-SNAPSHOT.dir/orientdb-community-3.0.0-SNAPSHOT/config/orientdb-server-config.xml\"/>"
                    + "<entry name=\"server.cache.staticResources\" value=\"false\"/>"
                    + "<entry name=\"log.console.level\" value=\"info\"/>"
                    + "<entry name=\"log.file.level\" value=\"fine\"/>"
                    // The following is required to eliminate an error or warning "Error on
                    // resolving
                    // property: ORIENTDB_HOME"
                    + "<entry name=\"plugin.dynamic\" value=\"false\"/>" + "</properties>" + "</orient-server>");
            // server.activate();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }



    }

    @BeforeClass
    public void setup() {
        factory = new OrientGraphFactory("remote:localhost/demo", "root", "cloud").setupPool(1, 10);
    }

    @Test
    public void testIdBug() {
        OrientGraph graph = factory.getTx();
        graph.setAutoStartTx(false);
        graph.begin();

        for (int i = 0; i < 5; i++) {
            graph.addVertex(CLASS_PREFIX + USER, createProperties());
        }
        graph.commit();
        graph.shutdown();
    }

    @Test
    public void testIdBug_concurrentModificationException() {
        OrientGraph graph = factory.getTx();
        graph.setAutoStartTx(false);
        graph.begin();
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.commit();
        graph.shutdown();
    }

    @Test
    public void testIdBug_multipleVertices_normal() {
        OrientGraph graph = factory.getTx();
        graph.setAutoStartTx(false);
        graph.begin();
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());

        graph.commit();
        graph.shutdown();
    }

    @Test
    public void testIdBug_multipleVertices() {
        OrientGraph graph = factory.getTx();
        graph.setAutoStartTx(false);
        graph.begin();
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
        graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());

        graph.commit();
        graph.shutdown();

    }

    private Map<String, Object> createProperties() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(NAME, "first +  last" + new Random().nextDouble());

        return props;
    }


    private Map<String, Object> createBonusProperties() {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(AMOUNT, new Random().nextDouble());

        return props;
    }



    // @Test
    /**
     * public void testIdbug_embedded() throws Exception { OrientGraph graph = new
     * OrientGraphFactory("plocal:c:/data/orientdb/databases/demo3", "root", "cloud") .setupPool(1,
     * 10).getTx(); graph.setAutoStartTx(false); graph.begin(); graph.addVertex(CLASS_PREFIX + USER,
     * createProperties()); graph.addVertex(CLASS_PREFIX + USER, createProperties());
     * graph.addVertex(CLASS_PREFIX + USER, createProperties()); graph.addVertex(CLASS_PREFIX +
     * USER, createProperties()); graph.addVertex(CLASS_PREFIX + USER, createProperties());
     * graph.commit();
     * 
     * 
     * 
     * }
     * 
     **/


}
