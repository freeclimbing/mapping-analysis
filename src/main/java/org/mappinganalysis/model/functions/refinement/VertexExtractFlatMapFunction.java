package org.mappinganalysis.model.functions.refinement;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.mappinganalysis.model.ObjectMap;

public class VertexExtractFlatMapFunction implements FlatMapFunction<Triplet<Long, ObjectMap, ObjectMap>,
    Vertex<Long, ObjectMap>> {
  @Override
  public void flatMap(Triplet<Long, ObjectMap, ObjectMap> triplet, Collector<Vertex<Long, ObjectMap>> collector)
      throws Exception {
    collector.collect(new Vertex<>(triplet.getSrcVertex().getId(), triplet.getSrcVertex().getValue()));
    collector.collect(new Vertex<>(triplet.getTrgVertex().getId(), triplet.getTrgVertex().getValue()));
  }
}
