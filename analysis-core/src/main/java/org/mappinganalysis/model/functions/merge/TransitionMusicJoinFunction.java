package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeMusicTriplet;

/**
 * Created by markus on 4/28/17.
 */
public class TransitionMusicJoinFunction
    implements JoinFunction<MergeMusicTriplet, Tuple2<Long, Long>, MergeMusicTriplet> {
  private Integer position;

  public TransitionMusicJoinFunction(Integer position) {
    this.position = position;
  }

  @Override
  public MergeMusicTriplet join(
      MergeMusicTriplet triplet,
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
