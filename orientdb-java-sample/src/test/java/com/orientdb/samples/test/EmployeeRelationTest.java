package com.orientdb.samples.test;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class EmployeeRelationTest {


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
    public void testInsertEmployeeVolumes() {

        OrientGraph graph = factory.getTx();
        // setupDbSchema(graph);
        // OrientGraph noTxGraph = factory.getNoTx();



        Vertex employeeVertex = graph.addVertex(T.label, "Employee", "name", "John");
        Vertex vol1 = graph.addVertex(T.label, "Volumes", "name", "PPV");
        Vertex vol2 = graph.addVertex(T.label, "Volumes", "name", "PV");
        Vertex vol3 = graph.addVertex(T.label, "Volumes", "name", "SPV");

        employeeVertex.addEdge("HAS_VOLUME", vol1);
        employeeVertex.addEdge("HAS_VOLUME", vol2);
        employeeVertex.addEdge("HAS_VOLUME", vol3);

        graph.commit();



    }


    @Test(priority = 2)
    public void testGetEmployeeVolumes() throws Exception {
        OrientGraph graph = factory.getTx();
        Vertex employee = getVertex(graph, "Employee", 1l);
        Long id = employee.value("id");
        List<String> volumeCodes = new ArrayList<String>();
        volumeCodes.add("PPV");
        volumeCodes.add("PV");
        volumeCodes.add("PGV");
        // Frame SQL IN Query
        String codes = volumeCodes.toString().replace("[", "").replace("]", "").replace(", ", "','");
        Map<String, Object> map = new HashMap<>();
        map.put("employeeId", id);
        map.put("codes", codes);
        // Use List
        // map.put("codes", volumeCodes);
        OGremlinResultSet resultSet = graph.executeSql(prepareInQuery(), map);
        List<Vertex> volumeVertexes = toVertices(graph, resultSet);
        for (Vertex vertex : volumeVertexes) {
            System.out.println("VolumeCode###" + vertex.value("name"));
        }
        graph.commit();
        assertEquals(2, volumeVertexes.size());


    }

    private String prepareInQuery() {
        // Single Query working as expected
        // "select from Volumes where name =:codes  and in('HAS_VOLUME').id CONTAINS :employeeId";
        // IN Query
        String query = "select from Volumes where name IN [:codes]  and in('HAS_VOLUME').id CONTAINS :employeeId";
        return query;
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
        noTxGraph
                .executeSql("CREATE PROPERTY BV.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BV.updatedDate DATETIME (MANDATORY FALSE);");
        noTxGraph.executeSql("CREATE CLASS BE EXTENDS E;");
        noTxGraph
                .executeSql("CREATE PROPERTY BE.createdDate DATETIME (MANDATORY TRUE, default sysdate(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\"));");
        noTxGraph.executeSql("CREATE PROPERTY BE.updatedDate DATETIME (MANDATORY FALSE);");
        noTxGraph.executeSql("CREATE CLASS Employee EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE  employeeIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Employee.name STRING ;");
        noTxGraph
                .executeSql("CREATE PROPERTY Employee.id LONG (MANDATORY TRUE, default \"sequence('employeeIdSequence').next()\");");
        noTxGraph.executeSql("CREATE CLASS Volumes EXTENDS BV;");
        noTxGraph.executeSql("CREATE SEQUENCE  volumesIdSequence TYPE ORDERED;");
        noTxGraph.executeSql("CREATE PROPERTY Volumes.name STRING ;");
        noTxGraph
                .executeSql("CREATE PROPERTY Volumes.id LONG (MANDATORY TRUE, default \"sequence('volumesIdSequence').next()\");");

        noTxGraph.executeSql("CREATE CLASS HAS_VOLUME EXTENDS BE;");



    }


}
