package org.mappinganalysis.model;


import org.apache.flink.api.java.tuple.Tuple3;

public class EdgeComponentTuple3 extends Tuple3<Long, Long, Long> {
  public EdgeComponentTuple3() {
  }

  public EdgeComponentTuple3(Long source, Long target, Long componentId) {
    super(source, target, componentId);
  }

  public Long getSourceId() {
    return f0;
  }

  public Long getTargetId() {
    return f1;
  }

  public Long getComponentId() {
    return f2;
  }}
