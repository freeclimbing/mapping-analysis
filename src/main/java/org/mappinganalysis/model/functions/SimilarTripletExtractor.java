package org.mappinganalysis.model.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;

/**
 * Return similarity 1f if labels of two resources are equal.
 */
public class SimilarTripletExtractor implements MapFunction<Triplet<Long, String, NullValue>,
    Triplet<Long, String, Float>> {
  @Override
  public Triplet<Long, String, Float> map(Triplet<Long, String, NullValue> triplet) throws Exception {
    boolean isSimilar = triplet.getSrcVertex().getValue().toLowerCase()
        .equals(triplet.getTrgVertex().getValue().toLowerCase());
    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            (isSimilar) ? 1f : 0f));
  }
}
