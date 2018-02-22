package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class AssignBlockIndex implements MapFunction<Tuple2<Long, Tuple2<String, Long>>, Tuple3<String, Long, Long>> {
  @Override
  public Tuple3<String, Long, Long> map(Tuple2<Long, Tuple2<String, Long>> input) throws Exception {
    return Tuple3.of(input.f1.f0, input.f1.f1, input.f0);
  }
}
