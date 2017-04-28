package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.JoinFunction;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;

/**
 * Created by markus on 4/28/17.
 */
public class TripletTupleMusicJoinFunction
    implements JoinFunction<MergeMusicTriplet, MergeMusicTuple, MergeMusicTriplet> {
  private final Integer position;

  public TripletTupleMusicJoinFunction(Integer position) {
    this.position = position;
  }

  @Override
  public MergeMusicTriplet join(MergeMusicTriplet triplet,
                              MergeMusicTuple newTuple) throws Exception {
    if (position == 0) {
      triplet.setSrcTuple(newTuple);
    } else if (position == 1) {
      triplet.setTrgTuple(newTuple);
    } else {
      throw new IllegalArgumentException("Unsupported position: " + position);
    }
    return triplet;
  }
}
