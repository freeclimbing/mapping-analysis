package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.mappinganalysis.model.IdTypeTuple;

public class EdgeTypeJoinFunction implements JoinFunction<Tuple4<Long, Long, String, String>,
    IdTypeTuple, Tuple4<Long, Long, String, String>> {
  private final int tuplePosition;

  public EdgeTypeJoinFunction(int tuplePosition) {
    this.tuplePosition = tuplePosition;
  }

  @Override
  public Tuple4<Long, Long, String, String> join(Tuple4<Long, Long, String, String> left,
                                                 IdTypeTuple right) throws Exception {
    if (tuplePosition == 0) {
      return new Tuple4<>(left.f0, left.f1, right.f1, "");
    } else {
      return new Tuple4<>(left.f0, left.f1, left.f2, right.f1);
    }
  }
}
