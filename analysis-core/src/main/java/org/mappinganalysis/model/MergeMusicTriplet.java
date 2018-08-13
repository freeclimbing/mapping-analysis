package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple6;
import org.mappinganalysis.util.Constants;

/**
 * L, L, T, T, sim, blocking label
 */
public class MergeMusicTriplet
    extends Tuple6<Long, Long, MergeTuple, MergeTuple, Double, String> {
  public MergeMusicTriplet() {
  }

  public MergeMusicTriplet(MergeTuple srcTuple,
                           MergeTuple trgTuple,
                           Double similarity) {
    this.f0 = srcTuple.getId();
    this.f1 = trgTuple.getId();
    this.f2 = srcTuple;
    this.f3 = trgTuple;
    this.f4 = similarity;
    this.f5 = Constants.EMPTY_STRING;
  }

  public MergeMusicTriplet(MergeTuple left,
                           MergeTuple right) {
    setIdAndTuples(left, right);
    this.f4 = 0d;
    this.f5 = Constants.EMPTY_STRING;
  }

  public MergeMusicTriplet(Long srcTuple,
                           Long trgTuple) {
    this.f0 = srcTuple;
    this.f1 = trgTuple;
    this.f2 = new MergeTuple(srcTuple);
    this.f3 = new MergeTuple(trgTuple);
    this.f4 = 0d;
    this.f5 = Constants.EMPTY_STRING;
  }

  /**
   * Sort left tuple to be the smaller id.
   */
  public void setIdAndTuples(MergeTuple left,
                             MergeTuple right) {
    if (left.getId() < right.getId()) {
      setSrcId(left.getId());
      setSrcTuple(left);
      setTrgId(right.getId());
      setTrgTuple(right);
    } else {
      setTrgId(left.getId());
      setTrgTuple(left);
      setSrcId(right.getId());
      setSrcTuple(right);
    }
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