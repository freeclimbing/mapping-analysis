package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Create edges based on idf values of entities.
 */
class IdfBasedEdgeCreator
    implements GroupReduceFunction<Tuple2<Long, Long>, Edge<Long, Integer>> {
  private final Edge<Long, Integer> reuseEdge;

  public IdfBasedEdgeCreator() {
    this.reuseEdge = new Edge<>();
    this.reuseEdge.setValue(1);
  }

  @Override
  public void reduce(
      Iterable<Tuple2<Long, Long>> values,
      Collector<Edge<Long, Integer>> out) throws Exception {
    ArrayList<Tuple2<Long, Long>> leftSide = Lists.newArrayList(values);
    ArrayList<Tuple2<Long, Long>> rightSide = Lists.newArrayList(leftSide);

    for (Tuple2<Long, Long> leftTuple : leftSide) {
      rightSide.remove(leftTuple);
      for (Tuple2<Long, Long> rightTuple : rightSide) {
        if (leftTuple.f0 < rightTuple.f0) {
          reuseEdge.setSource(leftTuple.f0);
          reuseEdge.setTarget(rightTuple.f0);
        } else {
          reuseEdge.setSource(rightTuple.f0);
          reuseEdge.setTarget(leftTuple.f0);
        }
//        System.out.println("IdfBasedEdgeCreator: " + reuseEdge.toString() + " " + rightTuple.f1);
        out.collect(reuseEdge);
      }
    }
  }
}
