package com.orientdb.samples.spring;

import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.springframework.stereotype.Repository;

@Repository("baseDao")
public interface BaseDao {

    /**
     * Return the
     * 
     * @return
     */
    OrientGraph getOrientGraph();


    public void closeGraph(OrientGraph orientGraph) throws Exception;

    public boolean executeQuery(OrientGraph graph, String queryString) throws Exception;


    public Vertex getVertex(OrientGraph orientGraph, String query, Map<String, Object> map) throws Exception;


    /**
     * Util for getting edge with the requested details
     * 
     * @param orientGraph contains the OrientDB connection
     * @param query contains the entity detail
     * @param map contains the filter detail
     * @return
     * @throws Exception
     */
    Edge getEdge(OrientGraph orientGraph, String query, Map<String, Object> map) throws Exception;

    public List<Vertex> getVertices(OrientGraph orientGraph, String query, Map<String, Object> map) throws Exception;

    public void closeResultset(OGremlinResultSet resultSet) throws Exception;

    boolean executeQuery(OrientGraph graph, String queryString, Map<String, Object> map) throws Exception;

    /**
     * Get the vertex of the given class with the given id.
     * 
     * @param orientGraph The graph object
     * @param className The Vertex class Name
     * @param id Id of the object in the db
     * @return If there is a vertex with given id, then the vertex is returned, otherwise null.
     * @throws MLMDaoException
     */
    Vertex getVertex(OrientGraph orientGraph, String className, Long id) throws Exception;


    public OrientGraph getNoTxGraph();
}
