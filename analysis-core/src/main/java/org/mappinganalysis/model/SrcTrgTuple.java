package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple2;

public class SrcTrgTuple extends Tuple2<Long, Long> {
  public SrcTrgTuple() {
  }

  public Long getSrc() {
    return f0;
  }

  public void setSrc(Long src) {
    f0 = src;
  }

  public Long getTrg() {
    return f1;
  }

  public void setTrg(Long trg) {
    f1 = trg;
  }
}
