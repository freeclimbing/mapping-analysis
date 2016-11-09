package org.mappinganalysis.model.api;

/**
 * Describes an entity with sources represented as integer value.
 */
public interface IntSources {
  /**
   * Returns the integer sources code of that entity.
   *
   * @return intSources
   */
  Integer getIntSources();

  /**
   * Sets the intSources of that entity.
   *
   * @param intSources type code to be set (must not be {@code null} or empty)
   */
  void setIntSources(Integer intSources);
}
