package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * MergeTuple consists of
 * 1. vertex id
 * 2. type (as int)
 * 3. label
 * 4. size
 * 5. sources (as int)
 */
public class MergeTuple extends Tuple5<Long, Integer, String, Integer, Integer> {
  public MergeTuple() {
  }

  public MergeTuple(Long vertexId, Integer intTypes, String label, Integer size, Integer intSources) {
    super(vertexId, intTypes, label, size, intSources);
  }

  public Long getVertexId() {
    return f0;
  }

  public void setVertexId(Long vertexId) {
    f0 = vertexId;
  }

  public Integer getIntTypes() {
    return f1;
  }

  public void setType(Integer type) {
    f1 = type;
  }

  public String getLabel() {
    return f2;
  }

  public void setLabel(String label) {
    f2 = label;
  }

  public Integer size() {
    return f3;
  }

  public void setSize(Integer size) {
    f3 = size;
  }

  public Integer getIntSources() {
    return f4;
  }

  public void setIntSources(Integer intSources) {
    f4 = intSources;
  }
}
