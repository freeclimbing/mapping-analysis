package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class ComputeBlockSize
    implements GroupReduceFunction<Tuple3<String, Integer, Long>, Tuple2<String, Long>> {
  @Override
  public void reduce(Iterable<Tuple3<String, Integer, Long>> input,
                     Collector<Tuple2<String, Long>> out) throws Exception {
    boolean isFirst = true;
    String key = null;
    long count = 0L;
    for (Tuple3<String, Integer, Long> i : input){
      if (isFirst) {
        key = i.f0;
        isFirst = false;
      }
      count += i.f2;
    }
    out.collect(Tuple2.of(key, count));
  }
}