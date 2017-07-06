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
public class AggSimValueEdgeMapFunction
    implements MapFunction<Edge<Long, ObjectMap>, Edge<Long, ObjectMap>> {
  private final boolean isMeanSimActivated;
  private String combination = "";

  /**
   * Aggregate all similarity values, either based on weight based metric
   * or simply by existence (missing properties are ignored).
   */
  public AggSimValueEdgeMapFunction(boolean isMeanSimActivated) {
    this.isMeanSimActivated = isMeanSimActivated;
  }

  /**
   * New constructor - music dataset currently
   */
  public AggSimValueEdgeMapFunction(String combination) {
    this.isMeanSimActivated = false;
    this.combination = combination;
  }

  @Override
  public Edge<Long, ObjectMap> map(Edge<Long, ObjectMap> edge) throws Exception {
    ObjectMap edgeValue = edge.getValue();
    Preconditions.checkArgument(!edgeValue.isEmpty(), "edge value empty: "
        + edge.getSource() + ", " + edge.getTarget());

    if (combination.equals(Constants.MUSIC)) {
      edgeValue.runOperation(new MeanAggregationFunction());
    } else { // old default
      double aggregatedSim;
      if (isMeanSimActivated) {
        aggregatedSim = SimilarityComputation.getMeanSimilarity(edgeValue);
      } else {
        aggregatedSim = SimilarityComputation.getWeightedAggSim(edgeValue);
      }
      edgeValue.setEdgeSimilarity(aggregatedSim);
    }

    return edge;
  }
}
