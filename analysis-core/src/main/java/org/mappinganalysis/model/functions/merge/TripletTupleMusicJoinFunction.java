package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;

/**
 * Created by markus on 4/28/17.
 */
public class TripletTupleMusicJoinFunction
    implements JoinFunction<MergeMusicTriplet, MergeMusicTuple, MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(TripletTupleMusicJoinFunction.class);

  private final Integer position;

  TripletTupleMusicJoinFunction(Integer position) {
    this.position = position;
  }

  @Override
  public MergeMusicTriplet join(MergeMusicTriplet triplet,
                              MergeMusicTuple newTuple) throws Exception {
//    if (newTuple.f0 < 10L)
//    LOG.info("CHANGES new Tuple: " + newTuple + " for triplet: " + triplet.toString());
    if (position == 0) {
      triplet.setSrcTuple(newTuple);
    } else if (position == 1) {
      triplet.setTrgTuple(newTuple);
    } else {
      throw new IllegalArgumentException("Unsupported position: " + position);
    }
//    if (triplet.f0 < 10L && triplet.f1 < 10L)
//    LOG.info("TripTupMuJoFu RESULTING CHANGES " + triplet.toString());

    return triplet;
  }
}
