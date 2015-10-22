package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;

/**
 * Flink vertex.
 *
 * f0: vertex identifier
 * f1: vertex label
 * f2: vertex properties
 */
public class FlinkVertex extends Tuple3<Integer, String, Map<String, Object>> {
  public Integer getVertexId() {
    return f0;
  }

  public void setVertexId(Integer vertexId) {
    f0 = vertexId;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String vertexLabel) {
    f1 = vertexLabel;
  }

  public Map<String, Object> getProperties() {
    return f2;
  }

  public void setProperties(Map<String, Object> properties) {
    f2 = properties;
  }
}
