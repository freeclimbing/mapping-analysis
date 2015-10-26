package org.mappinganalysis.model;

import org.apache.flink.graph.Vertex;

/**
 * Flink vertex.
 *
 * f0: vertex identifier
 * f1: vertex properties
 */
public class FlinkVertex extends Vertex<Long, PropertyContainer> {

  public FlinkVertex() {
  }

  public Long getId() {
    return f0;
  }

  public void setId(Long vertexId) {
    f0 = vertexId;
  }

  public PropertyContainer getValue() {
    return f1;
  }

  public void setValue(PropertyContainer properties) {
    f1 = properties;
  }
}
