package org.mappinganalysis.util.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

public class LeftMinusRightFunction<O, T> implements FlatJoinFunction<O, T, O> {
  private static final Logger LOG = Logger.getLogger(LeftMinusRightFunction.class);
  private final String s;

  public LeftMinusRightFunction(String s) {
    this.s = s;
  }

  @Override
  public void join(O left, T right, Collector<O> collector) throws Exception {
//    if (right != null) {
//      LOG.info("EXCLUDE " + s + left.toString());
//    }
    if (right != null) {
      collector.collect(left);
    }
  }
}