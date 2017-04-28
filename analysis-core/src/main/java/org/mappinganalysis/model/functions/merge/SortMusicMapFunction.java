package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;

/**
 */
class SortMusicMapFunction
    implements MapFunction<MergeMusicTriplet, MergeMusicTriplet> {
  @Override
  public MergeMusicTriplet map(MergeMusicTriplet triplet) throws Exception {
    if (triplet.getSrcId() > triplet.getTrgId()) {
      MergeMusicTuple tmpTuple = triplet.getSrcTuple();

      triplet.setSrcId(triplet.getTrgId());
      triplet.setSrcTuple(triplet.getTrgTuple());

      triplet.setTrgId(tmpTuple.getId());
      triplet.setTrgTuple(tmpTuple);
    }
    return triplet;
  }
}
