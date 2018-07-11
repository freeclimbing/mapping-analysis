package org.mappinganalysis.model.functions.clusterstrategies;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Remove if not used up to 18/8
 */
@Deprecated
class AddLeftSimStringJoinFunction
    implements JoinFunction<Tuple2<Long,Long>, Tuple2<Long, String>, Tuple3<Long, Long, String>> {
  @Override
  public Tuple3<Long, Long, String> join(
      Tuple2<Long, Long> first, Tuple2<Long, String> second) throws Exception {
    return new Tuple3<>(first.f0, first.f1, second.f1);
  }
}
