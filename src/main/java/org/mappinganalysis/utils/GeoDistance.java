package org.mappinganalysis.utils;

/**
 * GeoDistance
 * http://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude-what-am-i-doi%5D
 */
public class GeoDistance {
  /**
   * Calculate distance between two points in latitude and longitude taking
   * into account height difference. Uses Haversine method as its base.
   *
   * @param lat1 latitude first geo point
   * @param lat2 latitude first geo point
   * @param lon1 longitude first geo point
   * @param lon2 latitude first geo point
   * @param el1 altitude point 1 in meters
   * @param el2 altitude point 2 in meters
   * @return distance in meter
   */
  public static double distance(double lat1, double lon1, double lat2,
                                double lon2, double el1, double el2) {
    final int earthRadius = 6371;

    Double latDistance = Math.toRadians(lat2 - lat1);
    Double lonDistance = Math.toRadians(lon2 - lon1);
    Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
        + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
        * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
    Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    double distance = earthRadius * c * 1000; // convert to meters
    double height = el1 - el2;

    return Math.sqrt(Math.pow(distance, 2) + Math.pow(height, 2));
  }

  /**
   * Return distance between 2 geo points in meter. Uses Haversine method as its base.
   * @param lat1 latitude geo point 1
   * @param lon1 longitude geo point 1
   * @param lat2 latitude geo point 2
   * @param lon2 longitude geo point 2
   * @return distance in meter
   */
  public static double distance(double lat1, double lon1, double lat2, double lon2) {
    return distance(lat1, lon1, lat2, lon2, 0.0, 0.0);
  }
}
