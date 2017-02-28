package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * srcId, trgId, srcTuple, trgTuple, sim, blocking label
 */
public class MergeTriplet
    extends Tuple6<Long, Long, MergeTuple, MergeTuple, Double, String> {
  public MergeTriplet() {
  }

  public MergeTriplet(MergeTuple srcTuple, MergeTuple trgTuple, Double similarity) {

    this.f2 = srcTuple;
    this.f3 = trgTuple;
    this.f4 = similarity;
  }

  public Long getSrcId() {
    return f0;
  }

  public void setSrcId(Long id) {
    f0 = id;
  }

  public Long getTrgId() {
    return f1;
  }

  public void setTrgId(Long id) {
    f1 = id;
  }

  public MergeTuple getSrcTuple() {
    return f2;
  }

  public void setSrcTuple(MergeTuple src) {
    f2 = src;
  }

  public MergeTuple getTrgTuple() {
    return f3;
  }

  public void setTrgTuple(MergeTuple trg) {
    f3 = trg;
  }

  public Double getSimilarity() {
    return f4;
  }

  public void setSimilarity(Double similarity) {
    f4 = similarity;
  }

  public String getBlockingLabel() {
    return f5;
  }

  public void setBlockingLabel(String label) {
    f5 = label;
  }
}
