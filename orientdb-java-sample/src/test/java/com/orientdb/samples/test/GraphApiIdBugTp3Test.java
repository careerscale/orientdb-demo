package com.orientdb.samples.test;

import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Test
public class GraphApiIdBugTp3Test {

  public static final  String CLASS_PREFIX = "class:";
  public static final  String USER         = "User";
  public static final  String NAME         = "name";
  private static final String AMOUNT       = "amount";
  private static final String BONUS        = "bonus";

  static {

  }

  private OrientGraphFactory factory = null;

  @BeforeClass
  public void setup() {
    factory = new OrientGraphFactory("remote:localhost/demo", "admin", "admin")

        .setupPool(1, 10);

  }

  @Test
  public void testIdBug() {
    OrientGraph graph = factory.getTx();
    graph.begin();

    for (int i = 0; i < 5; i++) {
      graph.addVertex(CLASS_PREFIX + USER, createProperties());
    }
    graph.commit();
    graph.close();
    verifyUniqueIds();
  }

  @Test
  public void testForConnectionPooling() {

    OrientGraph graph = null;

    for (int n = 0; n < 100; n++) {
      graph = factory.getTx();
      graph.begin();

      for (int i = 0; i < 5; i++) {
        graph.addVertex(CLASS_PREFIX + USER, createProperties());
      }
      graph.commit();
      graph.close();
    }

    verifyUniqueIds();

  }

  private void verifyUniqueIds() {

    OrientGraph graph = factory.getTx();
    OResultSet vertices = graph.executeSql("select from User");
    List<Long> ids = Lists.newArrayList();

    vertices.vertexStream()
        .forEach(v -> {
              Long id = v.getProperty("id");
              Assert.assertFalse(ids.contains(id), "id is duplicate : " + id);
              ids.add(id);

            }

        );

    graph.close();
  }

  @Test
  public void testIdBug_concurrentModificationException() {
    OrientGraph graph = factory.getTx();
    graph.begin();
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.commit();
    graph.close();
  }

  @Test
  public void testIdBug_multipleVertices_normal() {
    OrientGraph graph = factory.getTx();
    graph.begin();
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());

    graph.commit();
    graph.close();
    verifyUniqueIds();
  }

  @Test
  public void testIdBug_multipleVertices() {
    OrientGraph graph = factory.getTx();
    graph.begin();
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());
    graph.addVertex(CLASS_PREFIX + USER, createProperties());
    graph.addVertex(CLASS_PREFIX + BONUS, createBonusProperties());

    graph.commit();
    graph.close();

    verifyUniqueIds();

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

}