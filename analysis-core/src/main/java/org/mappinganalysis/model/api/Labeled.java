package org.mappinganalysis.model.api;

/**
 * Describes an entity that has a label.
 */
public interface Labeled {
  /**
   * Returns the label of that entity.
   *
   * @return label
   */
  String getLabel();

  /**
   * Sets the label of that entity.
   *
   * @param label label to be set (must not be {@code null} or empty)
   */
  void setLabel(String label);
}