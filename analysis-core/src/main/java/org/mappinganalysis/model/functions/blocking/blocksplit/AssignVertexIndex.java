package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTuple;

public class AssignVertexIndex
    implements GroupReduceFunction<
    Tuple4<MergeMusicTuple, String, Integer, Long>,
    Tuple3<MergeMusicTuple, String, Long>> {
  @Override
  public void reduce(Iterable<Tuple4<MergeMusicTuple, String, Integer, Long>> input,
                     Collector<Tuple3<MergeMusicTuple, String, Long>> out) throws Exception {
    Long count = 0L;
    for (Tuple4<MergeMusicTuple, String, Integer, Long> tuple : input){
      out.collect(Tuple3.of(tuple.f0, tuple.f1, count+tuple.f3));
      count++;
    }
  }
}