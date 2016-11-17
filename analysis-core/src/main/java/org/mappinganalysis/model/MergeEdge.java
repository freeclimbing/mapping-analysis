package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple3;
import org.mappinganalysis.model.api.Similar;

public class MergeEdge extends Tuple3<Double, Boolean, Long> implements Similar{
  @Override
  public Double getSimilarity() {
    return f0;
  }

  @Override
  public void setSimilarity(Double similarity) {
    f0 = similarity;
  }
}
