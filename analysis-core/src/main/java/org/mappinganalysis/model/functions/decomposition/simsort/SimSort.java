package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;

public class SimSort
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private final ExecutionEnvironment env;
  private final Boolean prepareEnabled;

  public SimSort(Boolean prepareEnabled, ExecutionEnvironment env) {
    this.prepareEnabled = prepareEnabled;
    this.env = env;
  }

  /**
   * Execute SimSort procedure based on vertex-centric-iteration, as preparation:
   * create all missing edges, addGraph default vertex sim values
   * @throws Exception
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
    if (prepareEnabled) {
      DataSet<Edge<Long, NullValue>> distinctEdges = graph
          .getVertices()
          .runOperation(new EdgeComputationVertexCcSet(new HashCcIdKeySelector()));

      graph = Graph
          .fromDataSet(graph.getVertices(), distinctEdges, env)
          .run(new BasicEdgeSimilarityComputation(Constants.DEFAULT_VALUE, env));
    }

    return graph.run(new SimSortVertexCentricIteration(env));
  }

  /**
   * create all missing edges, addGraph default vertex sim values
   */
  @Deprecated
  public static Graph<Long, ObjectMap, ObjectMap> prepare(Graph<Long, ObjectMap, ObjectMap> graph,
                                                          ExecutionEnvironment env) throws Exception {
    DataSet<Edge<Long, NullValue>> distinctEdges = graph.getVertices()
        .runOperation(new EdgeComputationVertexCcSet(new HashCcIdKeySelector()));

    return Graph.fromDataSet(graph.getVertices(), distinctEdges, env)
        .run(new BasicEdgeSimilarityComputation(Constants.DEFAULT_VALUE, env));
  }
}
