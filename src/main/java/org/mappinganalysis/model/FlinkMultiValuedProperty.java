package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.List;

/**
 * Represents a multi-valued Flink property (e.g. rdf:type).
 *
 * Multi-values properties occur on vertices exclusively.
 *
 * f0: vertex id which owns the property
 * f1: property key
 * f2: list of property values
 */
public class FlinkMultiValuedProperty extends Tuple3<Long, String, List<Object>> {
  public Long getVertexId() {
    return f0;
  }

  public void setVertexId(Long vertexId) {
    f0 = vertexId;
  }

  public String getPropertyKey() {
    return f1;
  }

  public void setPropertyKey(String propertyKey) {
    f1 = propertyKey;
  }

  public List<Object> getPropertyValues() {
    return f2;
  }

  public void setPropertValues(List<Object> propertyValues) {
    f2 = propertyValues;
  }
}