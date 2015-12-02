package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ExcludeInputDataJoinFunction implements FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {
  @Override
  public void join(Tuple2<Long, Long> left, Tuple2<Long, Long> right,
                   Collector<Tuple2<Long, Long>> collector) throws Exception {
    if (right == null) {
      collector.collect(left);
    }
  }
}
