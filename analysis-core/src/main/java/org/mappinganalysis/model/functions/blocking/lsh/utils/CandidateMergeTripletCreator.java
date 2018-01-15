package org.mappinganalysis.model.functions.blocking.lsh.utils;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.util.AbstractionUtils;

public class CandidateMergeTripletCreator
    implements FlatJoinFunction<MergeGeoTriplet, MergeGeoTuple, MergeGeoTriplet> {
  private int side;

  public CandidateMergeTripletCreator(int side) {
    this.side = side;
  }

  @Override
  public void join(MergeGeoTriplet triplet, MergeGeoTuple mergeTuple,
                   Collector<MergeGeoTriplet> out) throws Exception {
    if (side == 0) {
      triplet.setSrcTuple(mergeTuple);

      out.collect(triplet);
    } else if (side == 1) {
      triplet.setTrgTuple(mergeTuple);

      if (!AbstractionUtils.hasOverlap(
          triplet.getSrcTuple().getIntSources(),
          triplet.getTrgTuple().getIntSources())) {
        out.collect(triplet);
      }
    }
  }
}
