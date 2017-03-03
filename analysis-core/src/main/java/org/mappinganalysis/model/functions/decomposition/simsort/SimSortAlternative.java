package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.mappinganalysis.model.ObjectMap;

/**
 * TODO
 */
public class SimSortAlternative implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private final ExecutionEnvironment env;
  private final Integer maxIterations;

  public SimSortAlternative(ExecutionEnvironment env) {
    this.env = env;
    this.maxIterations = Integer.MAX_VALUE;
  }

  /**
   * TODO split to altSimSort
   * Alternative sim-based refinement algorithm based on searching for cluster partitioning
   * with good average cluster similarity in sub clusters.
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> input) throws Exception {

      // TODO prepare is ok, perhaps delete property Constants.VERTEX_AGG_SIM_VALUE
      // TODO for alternative version, unneeded

      return input;
  }
}
