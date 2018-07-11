package org.mappinganalysis.model.functions.blocking.blocksplit;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

public class ConcatAllInfoToVertex
    implements JoinFunction<Tuple3<Long, String, Long>,
    Tuple5<String, Long, Long, Long, Long>,
    Tuple6<Long, String, Long, Long, Long, Long>> {
  /**
   * @param left f0 tuple id
   * @param right
   * @return output f0 tuple id
   */
  public Tuple6<Long, String, Long, Long, Long, Long> join(
      Tuple3<Long, String, Long> left,
      Tuple5<String, Long, Long, Long, Long> right) throws Exception {
    return Tuple6.of(left.f0, left.f1, left.f2, right.f1, right.f3, right.f4);
  }
}