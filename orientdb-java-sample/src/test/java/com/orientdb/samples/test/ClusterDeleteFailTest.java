package com.orientdb.samples.test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

public class ClusterDeleteFailTest {


    private static final String REPORTS_TO = "REPORTS_TO";
    private static final String CLUSTER1 = "employee_100";
    private static final String CLUSTER2 = "employee_200";

    private static OrientGraphFactory factory = null;

    @BeforeClass
    public static OrientGraphFactory setup() {

        factory = new OrientGraphFactory("memory:mlm", "admin", "admin").setupPool(1, 10);

        OrientGraph graph = factory.getNoTx();

        setupDbSchema(graph);
        // graph.commit();

        return factory;

    }

    @Test(priority = 1)
    public void testClusterCopy() {

        OrientGraph graph = factory.getTx();


        graph.getRawDatabase().command("ALTER  CLASS Employee ADDCLUSTER employee_100").close();
        graph.getRawDatabase().command("ALTER  CLASS Employee ADDCLUSTER employee_200").close();

        Vertex e1 = addEmployee("John", CLUSTER1, graph);
        Vertex e2 = addEmployee("John1", CLUSTER1, graph);
        Vertex e3 = addEmployee("John2", CLUSTER1, graph);
        Vertex e4 = addEmployee("Joh3", CLUSTER1, graph);


        e1.addEdge("REPORTS_TO", e2);
        e2.addEdge("REPORTS_TO", e3);
        e3.addEdge("REPORTS_TO", e4);

        graph.commit();
        getCount(graph);

        graph.getRawDatabase().command("delete vertex cluster:" + CLUSTER2).close();

        getCount(graph);
        getClusterSize(graph, CLUSTER1);
        getClusterSize(graph, CLUSTER2);
        copyDataToCluster(graph, e1, CLUSTER2);
        graph.commit();
        getCount(graph);
        getClusterSize(graph, CLUSTER1);
        getClusterSize(graph, CLUSTER2);
        copyDataToCluster(graph, e1, CLUSTER2);
        graph.commit();

        getCount(graph);



    }

    private Vertex addEmployee(String name, String cluster, OrientGraph graph) {
        Vertex e1 = graph.addVertex("Employee", CLUSTER1);
        e1.property("name", name);
        return e1;
    }


    private void getClusterSize(OrientGraph graph, String clusterName) {
        try {
            OResultSet resultSet = graph.getRawDatabase().execute("sql", "select count(*)  as count  from cluster:" + clusterName);
            if (resultSet.hasNext()) {
                OResult result = resultSet.next();
                System.out.println(clusterName + " data count :" + result.getProperty("count"));
            } else {
                System.out.println(clusterName + " data count not available");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }


    }

    private void getCount(OrientGraph graph) {
        try {
            OResultSet resultSet = graph.getRawDatabase().execute("sql", "select count(*)  as count  from REPORTS_TO");
            if (resultSet.hasNext()) {
                OResult result = resultSet.next();
                System.out.println("data count :" + result.getProperty("count"));
            } else {
                System.out.println("data count not available");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }


    }

    private Vertex copyDataToCluster(OrientGraph graph, Vertex e, String clusterName) {
        Vertex v = graph.addVertex("Employee", clusterName);
        v.property("name", e.property("name").isPresent() ? e.property("name").value() : "nothing yet");

        Iterator<Edge> edges = e.edges(Direction.OUT, REPORTS_TO);
        while (edges.hasNext()) {

            Vertex child = copyDataToCluster(graph, edges.next().inVertex(), clusterName);
            v.addEdge("REPORTS_TO", child);
            System.out.println("child created");
        }
        System.out.println("Record created");
        graph.commit();
        return v;
    }


    private Vertex getVertex(OrientGraph orientGraph, String className, Long id) throws Exception {

        String query = new StringBuilder("Select from ").append(className).append(" WHERE id =?").toString();
        OGremlinResultSet resultSet = orientGraph.executeSql(query, id);
        Vertex vertex = toVertex(orientGraph, resultSet);

        resultSet.close();
        return vertex;

    }

    private Vertex toVertex(OrientGraph graph, OGremlinResultSet resultSet) {
        if (resultSet.iterator().hasNext()) {
            return resultSet.iterator().next().getVertex().orElse(null);
        }
        return null;

    }

    public List<Vertex> toVertices(OrientGraph graph, OGremlinResultSet resultSet) {
        List<Vertex> vertices = new ArrayList<>();
        while (resultSet.iterator().hasNext()) {
            vertices.add(resultSet.iterator().next().getVertex().orElse(null));
        }
        return vertices;
    }



    private static void setupDbSchema(OrientGraph noTxGraph) {

        noTxGraph.executeSql("ALTER DATABASE DATETIMEFORMAT \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"");
        noTxGraph.executeSql("CREATE CLASS BV EXTENDS V");
        noTxGraph.executeSql("CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE);");
        noTxGraph.executeSql("CREATE CLASS BE EXTENDS E;");
        noTxGraph.executeSql("CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);");
        noTxGraph.executeSql("CREATE CLASS Employee EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE  employeeIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Employee.name STRING ;");
        noTxGraph.executeSql("CREATE PROPERTY Employee.id LONG (MANDATORY TRUE, default \"sequence('employeeIdSequence').next()\");");

        noTxGraph.executeSql("CREATE CLASS REPORTS_TO EXTENDS BE;");



    }


}
