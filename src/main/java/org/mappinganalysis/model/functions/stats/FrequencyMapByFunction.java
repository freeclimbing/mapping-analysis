package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class FrequencyMapByFunction implements MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
  private final int field;
  Tuple2<Long, Long> reuseTuple;

  public FrequencyMapByFunction(int field) {
    this.field = field;
    this.reuseTuple = new Tuple2<>();
  }

  @Override
  public Tuple2<Long, Long> map(Tuple2<Long, Long> tuple) throws Exception {
    if (field == 1) {
      reuseTuple.setFields(tuple.f1, 1L);
      return reuseTuple;
    } else {
      reuseTuple.setFields(tuple.f0, 1L);
      return reuseTuple;
    }
  }
}
