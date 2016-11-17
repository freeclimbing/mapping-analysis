package org.mappinganalysis.model.api;

/**
 * Describes an entity that has a similarity.
 */
public interface Similar {
  /**
   * Returns the similarity of that entity.
   *
   * @return label
   */
  Double getSimilarity();

  /**
   * Sets the similarity of that entity.
   *
   * @param similarity similarity to be set (must not be {@code null} or empty)
   */
  void setSimilarity(Double similarity);
}
