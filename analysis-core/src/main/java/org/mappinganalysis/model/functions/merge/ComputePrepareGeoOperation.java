package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;

/**
 */
public class ComputePrepareGeoOperation
    implements CustomUnaryOperation<MergeGeoTriplet, MergeGeoTriplet> {
  private DataSet<MergeGeoTriplet> triplets;
  private DataDomain domain;
  private int sourcesCount;

  public ComputePrepareGeoOperation(DataDomain domain, int sourcesCount) {
    this.domain = domain;
    this.sourcesCount = sourcesCount;
  }

  @Override
  public void setInput(DataSet<MergeGeoTriplet> inputData) {
    this.triplets = inputData;
  }

  @Override
  public DataSet<MergeGeoTriplet> createResult() {
    return triplets
        .map(new SortGeoMapFunction())
        .distinct(0,1) // needed
        .filter(new CheckRestrictionsFilterFunction<>(domain, sourcesCount));
  }

}
