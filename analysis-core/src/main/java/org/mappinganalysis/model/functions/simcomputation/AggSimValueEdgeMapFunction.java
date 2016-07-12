package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

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
    ObjectMap edgeValue = edge.getValue();

    Preconditions.checkArgument(!edgeValue.isEmpty(), "edge value empty: "
        + edge.getSource() + ", " + edge.getTarget());

    double aggregatedSim;
    if (ignoreMissingProperties) {
      aggregatedSim = SimilarityComputation.getMeanSimilarity(edgeValue);
    } else {
      aggregatedSim = SimilarityComputation.getWeightedAggSim(edgeValue);
    }

    edgeValue.put(Constants.AGGREGATED_SIM_VALUE, aggregatedSim);
    return edge;
  }
}
