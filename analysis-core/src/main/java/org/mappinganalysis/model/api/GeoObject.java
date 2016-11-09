package org.mappinganalysis.model.api;

/**
 * Describes an object which has geo coordinates
 */
public interface GeoObject {
  /**
   * Returns the latitude of that entity.
   *
   * @return latitude
   */
  public Double getLatitude();

  /**
   * Returns the longitude of that entity.
   *
   * @return longitude
   */
  public Double getLongitude();

  /**
   * Set the latitude of that entity.
   *
   * @param latitude latitude to be set (must not be {@code null} or empty)
   */
  void setLatitude(Double latitude);

  /**
   * Set the longitude of that entity.
   *
   * @param longitude longitude to be set (must not be {@code null} or empty)
   */
  void setLongitude(Double longitude);

}
