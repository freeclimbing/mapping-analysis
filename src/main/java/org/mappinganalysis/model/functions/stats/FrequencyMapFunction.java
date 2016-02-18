package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FrequencyMapFunction implements MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
  @Override
  public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple) throws Exception {
    return new Tuple2<>(tuple.f1, 1L);
  }
}
