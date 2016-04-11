package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.utils.Utils;

/**
 * Aggregate all similarity values, either based on weight based metric
 * or simply by existence (missing properties are ignored).
 */
public class AggSimValueEdgeMapFunction implements MapFunction<Edge<Long, ObjectMap>, Edge<Long, ObjectMap>> {
  private final boolean ignoreMissingProperties;

  public AggSimValueEdgeMapFunction(boolean ignoreMissingProperties) {
    this.ignoreMissingProperties = ignoreMissingProperties;
  }

  @Override
  public Edge<Long, ObjectMap> map(Edge<Long, ObjectMap> edge) throws Exception {
    ObjectMap value = edge.getValue();

    Preconditions.checkArgument(!value.isEmpty(), "edge value empty: " + edge.getSource() + ", "
    + edge.getTarget());

    double aggregatedSim;
    if (ignoreMissingProperties) {
      aggregatedSim = SimilarityComputation.getMeanSimilarity(value);
    } else {
      aggregatedSim = SimilarityComputation.getWeightedAggSim(value);
    }

    value.put(Utils.AGGREGATED_SIM_VALUE, aggregatedSim);
    return edge;
  }
}
