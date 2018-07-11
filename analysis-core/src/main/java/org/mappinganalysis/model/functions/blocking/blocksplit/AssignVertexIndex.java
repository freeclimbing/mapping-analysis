package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class AssignVertexIndex
    implements GroupReduceFunction<
    Tuple4<Long, String, Integer, Long>,
    Tuple3<Long, String, Long>> {
  /**
   * input and output f0 is tuple id
   */
  @Override
  public void reduce(Iterable<Tuple4<Long, String, Integer, Long>> input,
                     Collector<Tuple3<Long, String, Long>> out) throws Exception {
    Long count = 0L;
    for (Tuple4<Long, String, Integer, Long> tuple : input){
      out.collect(Tuple3.of(tuple.f0, tuple.f1, count+tuple.f3));
      count++;
    }
  }
}