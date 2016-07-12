package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.model.ObjectMap;

/**
 * Join the result of two different matching results.
 */
public class FullOuterJoinSimilarityValueFunction implements JoinFunction<Triplet<Long, ObjectMap, ObjectMap>,
    Triplet<Long, ObjectMap, ObjectMap>, Triplet<Long, ObjectMap, ObjectMap>> {
  @Override
  public Triplet<Long, ObjectMap, ObjectMap> join( Triplet<Long, ObjectMap, ObjectMap> left,
      Triplet<Long, ObjectMap, ObjectMap> right) throws Exception {

    Vertex<Long, ObjectMap> source;
    Vertex<Long, ObjectMap> target;
    if (left != null) {
      source = left.getSrcVertex();
      target = left.getTrgVertex();
    } else {
      source = right.getSrcVertex();
      target = right.getTrgVertex();
    }

    ObjectMap result = new ObjectMap();
    if (left != null) {
      result.putAll(left.getEdge().getValue());
    }
    if (right != null) {
      result.putAll(right.getEdge().getValue());
    }

    return new Triplet<>(
        source,
        target,
        new Edge<>(
            source.getId(),
            target.getId(),
            result));
  }
}
