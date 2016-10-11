package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.tuple.Tuple1;

/**
 * Helper class for SimSort vertex centric iteration
 */
public class SimSortEdgeTuple extends Tuple1<Double> {
  public SimSortEdgeTuple() {
  }

  public SimSortEdgeTuple(Double edgeSimilarity) {
    super(edgeSimilarity);
  }

  public void setSim(Double value) {
    this.f0 = value;
  }

  public Double getSim() {
    return f0;
  }
}
