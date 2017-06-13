package org.mappinganalysis.model.functions.merge;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeMusicTriplet;

/**
 */
public class MaxSimMinIdMusicReducer
    implements ReduceFunction<MergeMusicTriplet> {
  private static final Logger LOG = Logger.getLogger(MaxSimMinIdGeoReducer.class);

  @Override
  public MergeMusicTriplet reduce(MergeMusicTriplet left,
                                  MergeMusicTriplet right) throws Exception {
//    LOG.info(left.toString() + " #### " + right.toString());

    if (Doubles.compare(left.getSimilarity(), right.getSimilarity()) > 0 ) {
//      LOG.info("### take left: " + left.toString());
      return left;
    } else if (Doubles.compare(left.getSimilarity(), right.getSimilarity()) == 0) {
//      LOG.info("### others");
      Long first = left.getSrcId() < left.getTrgId() ? left.getSrcId() : left.getTrgId();
      Long second = right.getSrcId() < right.getTrgId() ? right.getSrcId() : right.getTrgId();
      if (first < second) {
//        LOG.info("### smaller left is smaller than small right, take left");
        return left;
      } else if (second < first) {
//        LOG.info("### smaller right is smaller than small left, take right");
        return right;
      }

      first = left.getSrcId() > left.getTrgId() ? left.getSrcId() : left.getTrgId();
      second = right.getSrcId() > right.getTrgId() ? right.getSrcId() : right.getTrgId();
      if (first < second) {
//        LOG.info("### bigger left is smaller than big right, take left");
        return left;
      } else {
//        LOG.info("### other big");
        return right;
      }
    } else {
//      LOG.info("### take right: " + right.toString());
      return right;
    }
  }
}
