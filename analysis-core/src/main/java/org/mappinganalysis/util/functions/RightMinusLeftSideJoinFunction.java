package org.mappinganalysis.util.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;

/**
 * Return all elements which have a match partner on the right dataset side.
 * @param <O>
 * @param <T>
 */
public class RightMinusLeftSideJoinFunction<O, T> implements FlatJoinFunction<O, T, T> {
  @Override
  public void join(O left, T right, Collector<T> collector) throws Exception {
    if (left == null) {
      collector.collect(right);
    }
  }
}
