package org.mappinganalysis.model.api;

/**
 * Describes an entity with a type represented as integer value.
 */
public interface IntTypes {
  /**
   * Returns the integer type code of that entity.
   *
   * @return intType
   */
  Integer getIntTypes();

  /**
   * Sets the intType of that entity.
   *
   * @param intType type code to be set (must not be {@code null} or empty)
   */
  void setIntTypes(Integer intType);
}
