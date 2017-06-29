package com.orientdb.samples.spring;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResult;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test
@DirtiesContext
@SpringBootTest(classes = Application.class)
@ComponentScan("com.orientdb.*")
public class EmployeeTest extends AbstractTestNGSpringContextTests {

    public static final String EMPLOYEE = "Employee";
    public static final String NAME = "name";
    public static final String STREET = "street";


    @Autowired
    BaseDao baseDao;
    OrientGraph graph = null;

    @BeforeClass
    public void setupTestData() throws Exception {
        // this should be excute first time to setup database schema.
        // setupDbSchema(baseDao.getNoTxGraph());
    }

    @Test(priority = 1, enabled = false)
    public void createEmployeee() {
        OrientGraph graph = baseDao.getOrientGraph();
        try {
            Vertex empVertex = graph.addVertex(T.label, EMPLOYEE, NAME, "Nagaraju", STREET, "Hacienda");
            OGremlinResultSet vertices = graph.executeSql("select from Country");
            Iterator<OGremlinResult> countryItr = vertices.iterator();
            while (countryItr.hasNext()) {
                empVertex.addEdge("LIVES_IN", countryItr.next().getVertex().get());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != graph && !graph.isClosed()) {
                graph.close();
            }
        }
    }

    @Test(priority = 2, enabled = false)
    public void updateEmployee_query() {
        OrientGraph graph = baseDao.getOrientGraph();
        try {
            OGremlinResultSet vertices = graph.executeSql("select from Employee");
            Iterator<OGremlinResult> empItr = vertices.iterator();
            int count = 0;
            while (empItr.hasNext()) {
                Vertex vertex = empItr.next().getVertex().get();
                Long id = vertex.value("id");
                String name = vertex.value("name");
                String updatedName = name + count;
                Map<String, Object> map = new HashMap<>();
                String query = "update LIVES_IN set status= '" + updatedName + "'  where out.id= " + id;
                System.out.println("QUERY ::" + query);
                graph.executeSql(query, map);
                count++;
                System.out.println("EmployeeTest.updateEmployeee() " + count);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != graph && !graph.isClosed()) {
                graph.close();
            }
        }
    }

    @Test(priority = 3, enabled = true)
    public void updateEmployee() {
        OrientGraph graph = baseDao.getOrientGraph();
        try {
            OGremlinResultSet vertices = graph.executeSql("select from Employee");
            Iterator<OGremlinResult> empItr = vertices.iterator();
            while (empItr.hasNext()) {
                Vertex vertex = empItr.next().getVertex().get();
                Iterator<Edge> edgeItr = vertex.edges(Direction.OUT, "LIVES_IN");
                while (edgeItr != null && edgeItr.hasNext()) {
                    Edge edge = edgeItr.next();
                    // Long empId = edge.inVertex().value("id");
                    edge.property("is_active", true);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != graph && !graph.isClosed()) {
                graph.close();
            }
        }
    }

    @Test(enabled = false)
    public void createCountry() {
        try {
            OrientGraph graph = baseDao.getOrientGraph();
            graph.begin();
            graph.addVertex(T.label, "Country", NAME, "India");
            graph.addVertex(T.label, "Country", NAME, "United States");
            graph.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupDbSchema(OrientGraph noTxGraph) {
        Map<String, Object> params = new HashMap<String, Object>();
        noTxGraph.executeSql("CREATE CLASS BV EXTENDS V;", params);
        noTxGraph.executeSql("CREATE CLASS Employee EXTENDS BV", params);
        noTxGraph.executeSql("CREATE SEQUENCE employeeIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Employee.id LONG (MANDATORY TRUE, default \"sequence('employeeIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE INDEX Employee.id ON Employee (id) UNIQUE");
        noTxGraph.executeSql("CREATE PROPERTY Employee.name STRING (MANDATORY TRUE, MIN 4, MAX 50);", params);
        noTxGraph.executeSql("CREATE PROPERTY Employee.street STRING (MANDATORY TRUE)", params);

        noTxGraph.executeSql("CREATE CLASS Country EXTENDS BV", params);
        noTxGraph.executeSql("CREATE SEQUENCE countryIdSequence TYPE ORDERED;", params);
        noTxGraph.executeSql(
                "CREATE PROPERTY Country.id LONG (MANDATORY TRUE, default \"sequence('countryIdSequence').next()\");",
                params);
        noTxGraph.executeSql("CREATE INDEX Country.id ON Country (id) UNIQUE");
        noTxGraph.executeSql("CREATE PROPERTY Country.name STRING (MANDATORY TRUE, MIN 4, MAX 50);", params);

        noTxGraph.executeSql("CREATE CLASS BE EXTENDS E;", params);
        noTxGraph.executeSql("CREATE CLASS LIVES_IN EXTENDS BE;", params);
        noTxGraph.executeSql("CREATE property LIVES_IN.out LINK Employee;", params);
        noTxGraph.executeSql("CREATE property LIVES_IN.status STRING;", params);

    }

}
