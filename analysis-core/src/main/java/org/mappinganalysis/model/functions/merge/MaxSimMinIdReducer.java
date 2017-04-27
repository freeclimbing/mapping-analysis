package org.mappinganalysis.model.functions.merge;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.MergeTriplet;

/**
 * If similarity is the same for several MergeTriplets, random value was taken previously.
 * Made post processing more complex. New:
 * Now we select the MergeTriplet, where the source or target id is the lowest.
 * TODO MergeTriplet smaller value is always first!? if yes, dont need all the lines
 */
class MaxSimMinIdReducer<T> implements ReduceFunction<MergeTriplet<T>> {
  private static final Logger LOG = Logger.getLogger(MaxSimMinIdReducer.class);

  @Override
  public MergeTriplet<T> reduce(MergeTriplet<T> left, MergeTriplet<T> right) throws Exception {
//    LOG.info(left.toString() + " #### " + right.toString());

    if (Doubles.compare(left.getSimilarity(), right.getSimilarity()) > 0 ) {
//      LOG.info("### take left");
      return left;
    } else if (Doubles.compare(left.getSimilarity(), right.getSimilarity()) == 0) {
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
//      LOG.info("### take right");
      return right;
    }
  }
}
