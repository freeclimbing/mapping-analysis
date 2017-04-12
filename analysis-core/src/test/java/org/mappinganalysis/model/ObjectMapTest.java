package org.mappinganalysis.model;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.util.Constants;

import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ObjectMapTest {
  private static final Logger LOG = Logger.getLogger(ObjectMapTest.class);

  @Test
  // do not create set of labels
  public void testGetLabel() throws Exception {
    ObjectMap test = new ObjectMap();

  }

  @Test
  public void testHasTypeNoType() throws Exception {
    ObjectMap objectMap = new ObjectMap();

    // initial no type
    objectMap.addProperty(Constants.TYPE_INTERN, Constants.NO_TYPE);
    assertTrue(objectMap.hasTypeNoType(Constants.TYPE_INTERN));

    objectMap.addProperty(Constants.TYPE_INTERN, "foo");
    objectMap.addProperty(Constants.TYPE_INTERN, Constants.NO_TYPE);
    assertFalse(objectMap.hasTypeNoType(Constants.TYPE_INTERN));

    objectMap.addProperty(Constants.TYPE_INTERN, "foo");
    objectMap.addProperty(Constants.TYPE_INTERN, "bar");

    // value added only once
    assertEquals(2, objectMap.getTypes(Constants.TYPE_INTERN).size());
    // initial no type can be overwritten
    assertFalse(objectMap.hasTypeNoType(Constants.TYPE_INTERN));


    objectMap.addProperty(Constants.TYPE_INTERN, Constants.NO_TYPE);
    assertFalse(objectMap.hasTypeNoType(Constants.TYPE_INTERN));
  }

  @Test
  public void testGetCcId() throws Exception {

  }

  @Test
  public void testGetTypes() throws Exception {

  }

  @Test
  public void testGetHashCcId() throws Exception {

  }

  @Test
  public void testGeoProperties() throws Exception {
    ObjectMap checkMap = new ObjectMap();
    ObjectMap input = new ObjectMap();
    input.addProperty(Constants.LAT, 23.0);
    input.addProperty(Constants.LON, 42.0);

    if (input.hasGeoPropertiesValid()) {
      HashMap<String, GeoCode> geoMap = Maps.newHashMap();
      geoMap.put("anything", new GeoCode(input.getLatitude(), input.getLongitude()));
      checkMap.setGeoProperties(geoMap);
      assertTrue(checkMap.hasGeoPropertiesValid());
    }

    input.remove(Constants.LAT);
    input.addProperty(Constants.LAT, 11111.0);
    assertFalse(input.hasGeoPropertiesValid());

  }

  @Test
  public void testGetVerticesList() throws Exception {
    ObjectMap map = new ObjectMap();
    Set<Long> set = Sets.newHashSet(1L, 2L);
    map.setClusterVertices(set);
    assertEquals(2, map.getVerticesList().size());
    map.addProperty(Constants.CL_VERTICES, 1L);
    assertEquals(2, map.getVerticesList().size());

    map.addProperty(Constants.CL_VERTICES, 3L);
    assertEquals(3, map.getVerticesList().size());
  }

  @Test
  public void testSimilarities() throws Exception {
    ObjectMap map = new ObjectMap();
    map.setEdgeSimilarity(0.5);
    assertEquals(0.5, map.getEdgeSimilarity(), 0.0001);
    map.setEdgeSimilarity(0.6);
    assertEquals(0.6, map.getEdgeSimilarity(), 0.0001);

    map.setVertexSimilarity(0.5);
    assertEquals(0.5, map.getVertexSimilarity(), 0.0001);
    map.setVertexSimilarity(0.6);
    assertEquals(0.6, map.getVertexSimilarity(), 0.0001);
  }

  @Test
  public void testClusterSources() throws Exception {
    ObjectMap map = new ObjectMap();
    Set<String> set = Sets.newHashSet(Constants.DBP_NS, Constants.FB_NS);
    map.setClusterDataSources(set);
    assertEquals(2, map.getDataSourcesList().size());
    map.addProperty(Constants.DATA_SOURCES, Constants.DBP_NS);
    assertEquals(2, map.getDataSourcesList().size());

    map.addProperty(Constants.DATA_SOURCES, Constants.NYT_NS);
    assertEquals(3, map.getDataSourcesList().size());
  }
}
