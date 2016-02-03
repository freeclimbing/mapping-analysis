package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple2;

public class AggSimValueTuple extends Tuple2<Double, Double> {

  public AggSimValueTuple() {
  }

  public AggSimValueTuple(Double vertexSim, Double edgeSim) {
    this.f0 = vertexSim;
    this.f1 = edgeSim;
  }

  public Double getVertexSim() {
    return f0;
  }

  public Double getEdgeSim() {
    return f1;
  }
}
