package org.mappinganalysis.util.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

/**
 * Return all elements which have a match partner on the right dataset side.
 * @param <T>
 * @param <O>
 */
public class LeftMinusRightSideJoinFunction<T, O> implements FlatJoinFunction<T, O, T> {
  private static final Logger LOG = Logger.getLogger(LeftMinusRightSideJoinFunction.class);

  @Override
  public void join(T left, O right, Collector<T> collector) throws Exception {
//    if (right != null) {
//      LOG.info("EXCLUDE: " + left.toString());
//    }
    if (right == null) {
//      LOG.info("HOLD: " + left.toString());
      collector.collect(left);
    }
  }
}
