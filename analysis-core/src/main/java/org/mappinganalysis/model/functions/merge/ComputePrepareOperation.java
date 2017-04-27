package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;

/**
 * Created by markus on 4/26/17.
 */
public class ComputePrepareOperation implements CustomUnaryOperation<MergeGeoTriplet, MergeGeoTriplet> {
  private DataSet<MergeGeoTriplet> triplets;
  private DataDomain domain;
  private int sourcesCount;

  public ComputePrepareOperation(DataDomain domain, int sourcesCount) {
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
        .map(new SortMapFunction())
        .distinct(0,1) // needed
        .filter(new CheckRestrictionsFilterFunction(sourcesCount));
  }

  private class SortMapFunction
      implements MapFunction<MergeGeoTriplet, MergeGeoTriplet> {
    @Override
    public MergeGeoTriplet map(MergeGeoTriplet triplet) throws Exception {
      if (triplet.getSrcId() > triplet.getTrgId()) {
        Object tmp = triplet.getSrcTuple();
        triplet.setSrcId(triplet.getTrgId());
        triplet.setSrcTuple(triplet.getTrgTuple());
//        if (domain == DataDomain.GEOGRAPHY) {
          MergeGeoTuple tmpTuple = (MergeGeoTuple) tmp;
          triplet.setTrgId(tmpTuple.getId());
          triplet.setTrgTuple(tmpTuple);
//        } else {
//          MergeMusicTuple tmpTuple = (MergeMusicTuple) tmp;
//          triplet.setTrgId(tmpTuple.getId());
//          triplet.setTrgTuple(tmpTuple);
//        }
      }
      return triplet;
    }
  }

}
