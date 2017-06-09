package org.mappinganalysis.model;

import org.apache.flink.api.java.tuple.Tuple6;
import org.mappinganalysis.util.Constants;

/**
 * L, L, T, T, sim, blocking label
 */
public class MergeMusicTriplet
    extends Tuple6<Long, Long, MergeMusicTuple, MergeMusicTuple, Double, String> {
  public MergeMusicTriplet() {
  }

  public MergeMusicTriplet(MergeMusicTuple srcTuple, MergeMusicTuple trgTuple, Double similarity) {
    this.f2 = srcTuple;
    this.f3 = trgTuple;
    this.f4 = similarity;
  }

  public MergeMusicTriplet(MergeMusicTuple left, MergeMusicTuple right) {
    setIdAndTuples(left, right);
    this.f4 = 0d;
    this.f5 = Constants.EMPTY_STRING;
  }

  /**
   * Sort left tuple to be the smaller id.
   */
  public void setIdAndTuples(MergeMusicTuple left, MergeMusicTuple right) {
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

  public MergeMusicTuple getSrcTuple() {
    return f2;
  }

  public void setSrcTuple(MergeMusicTuple src) {
    f2 = src;
  }

  public MergeMusicTuple getTrgTuple() {
    return f3;
  }

  public void setTrgTuple(MergeMusicTuple trg) {
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