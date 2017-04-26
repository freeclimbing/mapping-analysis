package org.mappinganalysis.model;

import org.mappinganalysis.util.AbstractionUtils;

import java.util.Set;

/**
 * merge tuple attributes interface
 */
public interface MergeTupleAttributes {
  Set<Long> getClusteredElements();

  void addClusteredElements(Set<Long> elements);

  Integer size();

  void setBlockingLabel(String label);

  String getBlockingLabel();

  boolean isActive();

  void setActive(Boolean value);
}
