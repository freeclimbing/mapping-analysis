package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.model.MergeGeoTriplet;

/**
 */
class TransitionGeoJoinFunction
    implements JoinFunction<MergeGeoTriplet, Tuple2<Long, Long>, MergeGeoTriplet> {
  private Integer position;

  public TransitionGeoJoinFunction(Integer position) {
    this.position = position;
  }

  @Override
  public MergeGeoTriplet join(
      MergeGeoTriplet triplet,
      Tuple2<Long, Long> transition) throws Exception {
    if (position == 0) {
      triplet.setSrcId(transition.f1);
    } else if (position == 1) {
      triplet.setTrgId(transition.f1);
    } else {
      throw new IllegalArgumentException("Unsupported position: " + position);
    }

    return triplet;
  }
}
