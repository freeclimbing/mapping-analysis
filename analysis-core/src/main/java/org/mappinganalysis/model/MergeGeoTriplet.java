package org.mappinganalysis.model;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple6;
import org.mappinganalysis.util.AbstractionUtils;
import org.mappinganalysis.util.Constants;

/**
 * srcId, trgId, srcTuple, trgTuple, sim, blocking label
 */
public class MergeGeoTriplet
    extends Tuple6<Long, Long, MergeGeoTuple, MergeGeoTuple, Double, String> {
  public MergeGeoTriplet() {
  }

  public MergeGeoTriplet(MergeGeoTuple srcTuple, MergeGeoTuple trgTuple, Double similarity) {
    this.f2 = srcTuple;
    this.f3 = trgTuple;
    this.f4 = similarity;
  }

  /**
   * Simple set id and tuples as given.
   */
  public void setIdAndTuples(MergeGeoTuple left, MergeGeoTuple right) {
    setSrcId(left.getId());
    setSrcTuple(left);

    setTrgId(right.getId());
    setTrgTuple(right);
  }

  /**
   * Set smaller source int value as left tuple in resulting triplet.
   */
  public void checkSourceSwitch(MergeGeoTuple left, MergeGeoTuple right, String newSource) {
    int newSourceInt = AbstractionUtils.getSourcesInt(Constants.GEO, Sets.newHashSet(newSource));

    if (AbstractionUtils.hasOverlap(left.getIntSources(), newSourceInt)) {//left.getIntSources() == newSourceInt) {
      MergeGeoTuple tmp = left;
      left = right;
      right = tmp;
    }

    setIdAndTuples(left, right);
  }

  @Deprecated
  public void checkSwitch(MergeGeoTuple left, MergeGeoTuple right) {
    if (left.getId() > right.getId()) {
      MergeGeoTuple tmp = left;
      left = right;
      right = tmp;
    }

    setIdAndTuples(left, right);
  }

  /**
   * Set one source to left tuple in resulting triplet.
   */
  public void checkSwitchSource(MergeGeoTuple left, MergeGeoTuple right) {

    //TODO

    setIdAndTuples(left, right);
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

  public MergeGeoTuple getSrcTuple() {
    return f2;
  }

  public void setSrcTuple(MergeGeoTuple src) {
    f2 = src;
  }

  public MergeGeoTuple getTrgTuple() {
    return f3;
  }

  public void setTrgTuple(MergeGeoTuple trg) {
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
