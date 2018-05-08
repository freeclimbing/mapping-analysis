package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Aggregate all similarity values, either based on weight based metric
 * or simply by existence (missing properties are ignored).
 */
public class AggSimValueEdgeMapFunction
    implements MapFunction<Edge<Long, ObjectMap>, Edge<Long, ObjectMap>> {
  private boolean isMeanSimActivated = false;
  private String mode = Constants.EMPTY_STRING;
  private DataDomain dataDomain;

  /**
   * Aggregate all similarity values, either based on weight based metric
   * or simply by existence (missing properties are ignored).
   */
  public AggSimValueEdgeMapFunction(boolean isMeanSimActivated) {
    this.isMeanSimActivated = isMeanSimActivated;
  }

  /**
   * New constructor - music/nc dataset currently
   */
  public AggSimValueEdgeMapFunction(String mode) {
    this.isMeanSimActivated = false;
    this.mode = mode;
  }

  public AggSimValueEdgeMapFunction(DataDomain dataDomain) {
    this.dataDomain = dataDomain;
  }

  public AggSimValueEdgeMapFunction() {
  }

  @Override
  public Edge<Long, ObjectMap> map(Edge<Long, ObjectMap> edge) throws Exception {
    ObjectMap edgeValue = edge.getValue();
    Preconditions.checkArgument(!edgeValue.isEmpty(), "edge value empty: "
        + edge.getSource() + ", " + edge.getTarget());

//    if (mode.equals(Constants.MUSIC) || mode.equals(Constants.NC)) {
      edgeValue.runOperation(new MeanAggregationFunction());
//    } else if (dataDomain == DataDomain.GEOGRAPHY) {
//      double aggregatedSim;
//        aggregatedSim = edgeValue
//            .runOperation(new MeanAggregationFunction())
//            .getEdgeSimilarity();
//      edgeValue.setEdgeSimilarity(aggregatedSim);
//    }

    return edge;
  }
}
