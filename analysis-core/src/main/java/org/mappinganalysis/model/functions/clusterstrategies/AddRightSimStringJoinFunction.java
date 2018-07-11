package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Remove if not used up to 18/8
 */
@Deprecated
class AddRightSimStringJoinFunction
    implements JoinFunction<
    Tuple3<Long,Long,String>,
    Tuple2<Long,String>,
    Tuple4<Long, Long, String, String>> {
  @Override
  public Tuple4<Long, Long, String, String> join(
      Tuple3<Long, Long, String> first, Tuple2<Long, String> second) throws Exception {
    return new Tuple4<>(first.f0, first.f1, first.f2, second.f1);
  }
}
