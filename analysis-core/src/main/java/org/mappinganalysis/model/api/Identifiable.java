package org.mappinganalysis.model.api;

/**
 * Describes an identifiable entity.
 */
public interface Identifiable {
  /**
   * Returns the identifier of that entity.
   *
   * @return identifier
   */
  Long getId();

  /**
   * Sets the identifier of that entity.
   *
   * @param id identifier to be set (must not be {@code null} or empty)
   */
  void setId(Long id);
}
