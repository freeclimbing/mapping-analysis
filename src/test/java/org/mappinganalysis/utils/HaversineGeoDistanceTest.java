package org.mappinganalysis.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Geo Distance Test
 */
public class HaversineGeoDistanceTest {

  @Test
  public void testDistance() throws Exception {

    // prague
    final double latitudePrague = 50.08804;
    final double longitudePrague = 14.42076;
    final double testEle = 123.45;
    // berlin
    final double latitudeBerlin = 52.52437;
    final double longitudeBerlin = 13.41053;
    final double testEleBerlin = 234.56;

    final double distancePrBe = 279853.933175651;

    double result = HaversineGeoDistance.distance(latitudePrague, longitudePrague, latitudeBerlin, longitudeBerlin);
    assertEquals(result, distancePrBe, 0.0);

    double equal = HaversineGeoDistance.distance(latitudePrague, longitudePrague, latitudePrague, longitudePrague);
    assertEquals(equal, 0.0, 0.0);

    double notEqual = HaversineGeoDistance.distance(latitudePrague, longitudePrague, latitudePrague, longitudePrague,
        testEle, testEleBerlin);
    assertNotEquals(notEqual, 0.0, 0.0);



  }
}