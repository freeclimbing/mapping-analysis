package org.mappinganalysis.model.functions.preprocessing;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class EdgeTypeJoinFunction implements JoinFunction<Tuple4<Long, Long, String, String>,
    Tuple2<Long, String>, Tuple4<Long, Long, String, String>> {
  private final int tuplePosition;

  public EdgeTypeJoinFunction(int tuplePosition) {
    this.tuplePosition = tuplePosition;
  }

  @Override
  public Tuple4<Long, Long, String, String> join(Tuple4<Long, Long, String, String> edge,
                                                 Tuple2<Long, String> tuple) throws Exception {
    if (tuplePosition == 0) {
      return new Tuple4<>(edge.f0, edge.f1, tuple.f1, "");
    } else {
      return new Tuple4<>(edge.f0, edge.f1, edge.f2, tuple.f1);
    }
  }
}
