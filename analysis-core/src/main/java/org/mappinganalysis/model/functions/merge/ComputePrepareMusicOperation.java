package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeMusicTriplet;

/**
 */
public class ComputePrepareMusicOperation
    implements CustomUnaryOperation<MergeMusicTriplet, MergeMusicTriplet> {
  private DataSet<MergeMusicTriplet> triplets;
  private DataDomain domain;
  private int sourcesCount;

  ComputePrepareMusicOperation(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void setInput(DataSet<MergeMusicTriplet> inputData) {
    this.triplets = inputData;
  }

  @Override
  public DataSet<MergeMusicTriplet> createResult() {
    return triplets
        .map(new SortMusicMapFunction())
        .distinct(0,1) // needed
        .filter(new CheckRestrictionsFilterFunction<>(domain, sourcesCount));
  }

}
