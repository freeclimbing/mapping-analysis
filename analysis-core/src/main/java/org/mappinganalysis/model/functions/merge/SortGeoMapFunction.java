package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;

/**
 */
public class SortGeoMapFunction
    implements MapFunction<MergeGeoTriplet, MergeGeoTriplet> {
  @Override
  public MergeGeoTriplet map(MergeGeoTriplet triplet) throws Exception {
    if (triplet.getSrcId() > triplet.getTrgId()) {
      MergeGeoTuple tmpTuple = triplet.getSrcTuple();
      triplet.setSrcId(triplet.getTrgId());
      triplet.setSrcTuple(triplet.getTrgTuple());
      triplet.setTrgId(tmpTuple.getId());
      triplet.setTrgTuple(tmpTuple);
    }
    return triplet;
  }
}
