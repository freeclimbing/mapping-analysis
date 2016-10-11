package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Helper class for SimSort vertex centric iteration
 */
public class SimSortVertexTuple extends Tuple4<Long, Long, Double, Boolean> {
  public SimSortVertexTuple() {
  }

  public SimSortVertexTuple(Long hash, Long oldHash, Double sim, Boolean state) {
    super(hash, oldHash, sim, state);
  }

  public Long getHash() {
    return f0;
  }

  public void setHash(Long value) {
    this.f0 = value;
  }

  public Long getOldHash() {
    return f1;
  }

  public void setOldHash(Long value) {
    this.f1 = value;
  }

  public Double getSim() {
    return f2;
  }

  public void setSim(Double value) {
    this.f2 = value;
  }

  public Boolean isActive() {
    return f3;
  }

  public void setActive(Boolean value) {
    this.f3 = value;
  }
}
