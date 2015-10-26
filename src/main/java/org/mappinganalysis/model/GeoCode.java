package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Geo coordinate object.
 */
public class GeoCode extends Tuple2<Double, Double> {
  public GeoCode() {

  }

  public GeoCode(double lat, double lon) {
    f0 = lat;
    f1 = lon;
  }

  public double getLat() {
    return f0;
  }

  public void setLat(double lat) {
    f0 = lat;
  }

  public double getLon() {
    return f1;
  }

  public void setLon(double lon) {
    f1 = lon;
  }
}
