package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple5;

import java.util.Map;

/**
 * Flink edge.
 *
 * f0: edge identifier
 * f1: edge label
 * f2: source vertex id
 * f3: target vertex id
 * f4: edge properties
 */
/**
 * not yet used
 */
@Deprecated
public class FlinkEdge extends
    Tuple5<Long, String, Long, Long, Map<String, Object>> {

  public Long getEdgeId() {
    return f0;
  }

  public void setEdgeId(Long edgeId) {
    f0 = edgeId;
  }

  public String getLabel() {
    return f1;
  }

  public void setLabel(String label) {
    f1 = label;
  }

  public Long getSourceVertexId() {
    return f2;
  }

  public void setSourceVertexId(Long sourceVertexId) {
    f2 = sourceVertexId;
  }

  public Long getTargetVertexId() {
    return f3;
  }

  public void setTargetVertexId(Long targetVertexId) {
    f3 = targetVertexId;
  }

  public Map<String, Object> getProperties() {
    return f4;
  }

  public void setProperties(Map<String, Object> properties) {
    f4 = properties;
  }
}
