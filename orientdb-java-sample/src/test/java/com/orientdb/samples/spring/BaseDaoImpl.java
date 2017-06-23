package com.orientdb.samples.spring;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public abstract class BaseDaoImpl implements BaseDao {


    @Autowired
    protected OrientDbConfig dbConfig;

    private static final Logger LOG = LoggerFactory.getLogger(BaseDaoImpl.class);

    private static final String FROM = "select from ";

    private static final Object WHERE_ID = " where id = ";

    private static final Object WHERE = " where ";

    @Override
    public OrientGraph getOrientGraph() {
        OrientGraph orientGraph = dbConfig.getOrientGraph();
        return orientGraph;

    }

    @Override
    public OrientGraph getNoTxGraph() {
        return dbConfig.getFactory().getNoTx();
    }

    @Override
    public void closeGraph(OrientGraph orientGraph) {
        if (orientGraph != null && !orientGraph.isClosed()) {
            try {
                orientGraph.close();
            } catch (Exception e) {
                LOG.warn("Unable to close graph {}", e);
            }
        }
    }

    @Override
    public void closeResultset(OGremlinResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Exception e) {
                LOG.warn("Unable to close resultSet {}", e);
            }
        }
    }



    /**
     * Base Method Dynamically get the Vertex Based on Id and Class Name Using Tinkerpop Graph API
     * 
     * @param orientGraph
     * @param className
     * @param id
     * @return
     * @throws MLMDaoException
     */
    @Override
    public Vertex getVertex(OrientGraph orientGraph, String className, Long id) throws Exception {

        String query = new StringBuilder(FROM).append(className).append(WHERE_ID).toString();
        OGremlinResultSet resultSet = orientGraph.executeSql(query, id);
        Vertex vertex = toVertex(orientGraph, resultSet);

        resultSet.close();
        return vertex;

    }

    protected Vertex getVertex(OrientGraph orientGraph, String className, String fieldName, String fieldValue)
            throws Exception {
        OGremlinResultSet resultSet = null;
        try {
            String query =
                    new StringBuilder(FROM).append(className).append(WHERE).append(fieldName).append(" = ?").toString();
            resultSet = orientGraph.executeSql(query, fieldValue);
            return toVertex(orientGraph, resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception e) {
                    LOG.error("error : {} ", e);
                }
            }
        }
    }

    @Override
    public Vertex getVertex(OrientGraph orientGraph, String query, Map<String, Object> map) throws Exception {
        OGremlinResultSet resultSet = null;
        try {
            resultSet = orientGraph.executeSql(query, map);
            return toVertex(orientGraph, resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            closeResultset(resultSet);
        }
    }

    @Override
    public Edge getEdge(OrientGraph orientGraph, String query, Map<String, Object> map) throws Exception {
        OGremlinResultSet resultSet = null;
        try {
            resultSet = orientGraph.executeSql(query, map);
            return toEdge(orientGraph, resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            closeResultset(resultSet);
        }
    }

    @Override
    public List<Vertex> getVertices(OrientGraph orientGraph, String query, Map<String, Object> map) throws Exception {
        OGremlinResultSet resultSet = null;
        try {
            resultSet = orientGraph.executeSql(query, map);
            return toVertices(orientGraph, resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            closeResultset(resultSet);
        }
    }

    protected List<Vertex> getVertices(OrientGraph orientGraph, String className, String fieldName, String fieldValue)
            throws Exception {
        OGremlinResultSet resultSet = null;
        try {
            String query =
                    new StringBuilder(FROM).append(className).append(WHERE).append(fieldName).append(" = ?").toString();
            resultSet = orientGraph.executeSql(query, fieldValue);
            return toVertices(orientGraph, resultSet);
        } catch (Exception e) {
            throw e;
        } finally {
            closeResultset(resultSet);
        }
    }


    @Override
    public boolean executeQuery(OrientGraph graph, String queryString) throws Exception {
        boolean isExecuted = false;

        if (StringUtils.isNotBlank(queryString)) {
            graph.getRawDatabase().command(queryString, new HashedMap<>());
            isExecuted = true;
        }
        return isExecuted;
    }

    @Override
    public boolean executeQuery(OrientGraph graph, String queryString, Map<String, Object> map) throws Exception {
        boolean isExecuted = false;
        try {
            if (StringUtils.isNotBlank(queryString)) {
                graph.getRawDatabase().command(queryString, map);
                isExecuted = true;
            }
        } catch (Exception e) {
            throw e;
        }
        return isExecuted;
    }

    public Vertex toVertex(OrientGraph graph, OGremlinResultSet resultSet) {
        if (resultSet.iterator().hasNext()) {
            return resultSet.iterator().next().getVertex().orElse(null);
        }
        return null;

    }

    public Edge toEdge(OrientGraph graph, OGremlinResultSet resultSet) {
        if (resultSet.iterator().hasNext()) {
            return resultSet.iterator().next().getEdge().orElse(null);
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

    protected List<Vertex> getOutVertices(Iterator<Edge> edges) {
        List<Vertex> vertices = new ArrayList<>();
        if (null != edges) {
            while (edges.hasNext()) {
                Edge edge = edges.next();
                Vertex vertex = (null != edge) ? edge.outVertex() : null;
                if (null != vertex) {
                    vertices.add(vertex);
                }
            }
        }
        return vertices;
    }

    protected List<Vertex> getInVertices(Iterator<Edge> edges) {
        List<Vertex> vertices = new ArrayList<>();
        if (null != edges) {
            while (edges.hasNext()) {
                Edge edge = edges.next();
                Vertex vertex = (null != edge) ? edge.inVertex() : null;
                if (null != vertex) {
                    vertices.add(vertex);
                }
            }
        }
        return vertices;
    }

    protected Vertex getOutVertex(Iterator<Edge> edges) {
        Vertex vertex = null;
        if (null != edges && edges.hasNext()) {
            Edge edge = edges.next();
            vertex = (null != edge) ? edge.outVertex() : null;
        }
        return vertex;
    }

    protected Vertex getInVertex(Iterator<Edge> edges) {
        Vertex vertex = null;
        if (null != edges && edges.hasNext()) {
            Edge edge = edges.next();
            vertex = (null != edge) ? edge.inVertex() : null;
        }
        return vertex;
    }


}
