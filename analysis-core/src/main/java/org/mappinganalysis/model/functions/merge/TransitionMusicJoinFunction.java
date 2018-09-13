package org.mappinganalysis.model.functions.merge;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;

/**
 * Join to create triplet.
 */
public class TransitionMusicJoinFunction
    implements JoinFunction<MergeTriplet, Tuple2<Long, Long>, MergeTriplet> {
  private static final Logger LOG = Logger.getLogger(TransitionMusicJoinFunction.class);

  private Integer position;

  TransitionMusicJoinFunction(Integer position) {
    this.position = position;
  }

  @Override
  public MergeTriplet join(
      MergeTriplet triplet,
      Tuple2<Long, Long> transition) throws Exception {
//    LOG.info("transitionjoins" + transition + " for " + triplet.toString());
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
