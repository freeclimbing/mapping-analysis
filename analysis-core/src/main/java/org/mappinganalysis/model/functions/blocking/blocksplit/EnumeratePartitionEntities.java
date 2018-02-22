package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.MergeMusicTuple;

public class EnumeratePartitionEntities
    implements GroupCombineFunction<
    Tuple3<MergeMusicTuple, String, Integer>,
    Tuple3<String, Integer, Long>> {
  @Override
  public void combine(Iterable<Tuple3<MergeMusicTuple, String, Integer>> input,
                      Collector<Tuple3<String, Integer, Long>> out) throws Exception {
    boolean isFirst = true;
    String blockingKey = null;
    int partitionId = 0;

    long count = 0L;
    for (Tuple3<MergeMusicTuple, String, Integer> tuple : input){
      if (isFirst) {
        blockingKey = tuple.f1;
        partitionId = tuple.f2;
        isFirst = false;
      }
      count++;
    }

    out.collect(Tuple3.of(blockingKey, partitionId, count));
  }
}