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
  public void testHasTypeNoType() throws Exception {
    ObjectMap props = new ObjectMap(Constants.GEO);

    // initial no type
    props.addProperty(Constants.TYPE_INTERN, Constants.NO_TYPE);
    assertTrue(props.hasTypeNoType(Constants.TYPE_INTERN));

    props.addProperty(Constants.TYPE_INTERN, "foo");
    props.addProperty(Constants.TYPE_INTERN, Constants.NO_TYPE);
    assertFalse(props.hasTypeNoType(Constants.TYPE_INTERN));

    props.addProperty(Constants.TYPE_INTERN, "foo");
    props.addProperty(Constants.TYPE_INTERN, "bar");

    // value added only once
    assertEquals(2, props.getTypes(Constants.TYPE_INTERN).size());
    // initial no type can be overwritten
    assertFalse(props.hasTypeNoType(Constants.TYPE_INTERN));


    props.addProperty(Constants.TYPE_INTERN, Constants.NO_TYPE);
    assertFalse(props.hasTypeNoType(Constants.TYPE_INTERN));
  }

  @Test
  public void testGeoProperties() throws Exception {
    ObjectMap checkMap = new ObjectMap(Constants.GEO);
    ObjectMap input = new ObjectMap(Constants.GEO);
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
    ObjectMap map = new ObjectMap(Constants.GEO);
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
    ObjectMap map = new ObjectMap(Constants.GEO);
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
    ObjectMap map = new ObjectMap(Constants.GEO);
    Set<String> set = Sets.newHashSet(Constants.DBP_NS, Constants.FB_NS);
    map.setClusterDataSources(set);
    assertEquals(2, map.getDataSourcesList().size());
    map.addProperty(Constants.DATA_SOURCES, Constants.DBP_NS);
    assertEquals(2, map.getDataSourcesList().size());

    map.addProperty(Constants.DATA_SOURCES, Constants.NYT_NS);
    assertEquals(3, map.getDataSourcesList().size());
  }
}
