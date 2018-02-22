package org.mappinganalysis.model.functions.stats;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Preprocessing class for aggregate sum count, either take column 0 or 1 as f0, f1 is always 1L.
 */
public class FrequencyMapByFunction implements MapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
  private final int field;
  private Tuple2<Long, Long> reuseTuple;

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
