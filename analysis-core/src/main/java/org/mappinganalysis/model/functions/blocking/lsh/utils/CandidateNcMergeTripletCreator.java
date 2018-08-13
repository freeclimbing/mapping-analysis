package org.mappinganalysis.model.functions.blocking.lsh.utils;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.util.AbstractionUtils;

public class CandidateNcMergeTripletCreator
    implements FlatJoinFunction<MergeMusicTriplet, MergeTuple, MergeMusicTriplet> {
  private int side;

  public CandidateNcMergeTripletCreator(int side) {
    this.side = side;
  }

  @Override
  public void join(MergeMusicTriplet triplet, MergeTuple mergeTuple,
                   Collector<MergeMusicTriplet> out) throws Exception {
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
