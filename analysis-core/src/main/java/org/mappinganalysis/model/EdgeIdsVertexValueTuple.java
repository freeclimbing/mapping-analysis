package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple4;

public class EdgeIdsVertexValueTuple extends Tuple4<Long, Long, String, String> {
  public EdgeIdsVertexValueTuple() {
  }

  public EdgeIdsVertexValueTuple(Long srcId, Long trgId, String srcSource, String trgSource) {
    super(srcId, trgId, srcSource, trgSource);
  }

  public Long getSrcId() {
    return f0;
  }

  public Long getTrgId() {
    return f1;
  }

  public String getSrcSource() {
    return f2;
  }

  public String getTrgSource() {
    return f3;
  }

  public void checkSideAndUpdate(int side, String sourceValue) {
    if (getSrcSource().isEmpty() && getTrgSource().isEmpty()) {
      if (side == 0) {
        this.f2 = sourceValue;
      } else  if (side == 1){
        this.f3 = sourceValue;
      }
    } else if (getSrcSource().isEmpty() && side == 0) {
      this.f2 = sourceValue;
    } else if (getTrgSource().isEmpty() && side == 1) {
      this.f3 = sourceValue;
    }

  }
}
