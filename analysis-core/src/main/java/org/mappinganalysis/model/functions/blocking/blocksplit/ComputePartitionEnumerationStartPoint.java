package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ComputePartitionEnumerationStartPoint
    implements GroupReduceFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>> {
  @Override
  public void reduce(Iterable<Tuple3<String, Integer, Long>> input,
                     Collector<Tuple3<String, Integer, Long>> out) throws Exception {
    long totalSum = 0L;
    for (Tuple3<String, Integer, Long> tuple : input){ // blocking key, partition id, count
      out.collect(Tuple3.of(tuple.f0, tuple.f1, totalSum));
      totalSum += tuple.f2;
    }
  }
}

