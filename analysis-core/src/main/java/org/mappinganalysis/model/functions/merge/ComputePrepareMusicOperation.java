package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeTriplet;

/**
 */
public class ComputePrepareMusicOperation
    implements CustomUnaryOperation<MergeTriplet, MergeTriplet> {
  private DataSet<MergeTriplet> triplets;
  private DataDomain domain;
  private int sourcesCount;

  ComputePrepareMusicOperation(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void setInput(DataSet<MergeTriplet> inputData) {
    this.triplets = inputData;
  }

  @Override
  public DataSet<MergeTriplet> createResult() {
    return triplets
        .map(new SortMusicMapFunction())
        .distinct(0,1) // needed
        .filter(new CheckRestrictionsFilterFunction<>(domain, sourcesCount));
  }

}
