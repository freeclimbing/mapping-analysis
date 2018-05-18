package org.mappinganalysis.model.functions.simcomputation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.mappinganalysis.model.ObjectMap;

/**
 * Mean Aggregate all similarity values, missing properties are ignored.
 */
public class AggSimValueEdgeMapFunction
    implements MapFunction<Edge<Long, ObjectMap>, Edge<Long, ObjectMap>> {

  @Override
  public Edge<Long, ObjectMap> map(Edge<Long, ObjectMap> edge) throws Exception {
    ObjectMap edgeValue = edge.getValue();
    if (edgeValue.isEmpty()) {
      edgeValue.setEdgeSimilarity(0d);
    } else {
      edgeValue.runOperation(new MeanAggregationFunction());
    }

    return edge;
  }
}
