package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * Create edges based on idf values of entities.
 */
class IdfBasedEdgeCreator
    implements GroupReduceFunction<Tuple2<Long, String>, Edge<Long, Integer>> {
  @Override
  public void reduce(
      Iterable<Tuple2<Long, String>> values,
      Collector<Edge<Long, Integer>> out) throws Exception {
    HashSet<Tuple2<Long, String>> leftSide = Sets.newHashSet(values);
    HashSet<Tuple2<Long, String>> rightSide = Sets.newHashSet(leftSide);

    for (Tuple2<Long, String> leftTuple : leftSide) {
      rightSide.remove(leftTuple);
      for (Tuple2<Long, String> rightTuple : rightSide) {
        if (leftTuple.f0 < rightTuple.f0) {
          out.collect(new Edge<>(leftTuple.f0, rightTuple.f0, 1));
        } else {
          out.collect(new Edge<>(rightTuple.f0, leftTuple.f0, 1));
        }
      }
    }
  }
}
