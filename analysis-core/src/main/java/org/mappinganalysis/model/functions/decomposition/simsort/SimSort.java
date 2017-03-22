package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.BasicEdgeSimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;

public class SimSort
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private static final Logger LOG = Logger.getLogger(SimSort.class);

  private final ExecutionEnvironment env;
  private final Boolean prepareEnabled;
  private final Double minSimilarity;

  public SimSort(Boolean prepareEnabled, Double minSimilarity, ExecutionEnvironment env) {
    this.prepareEnabled = prepareEnabled;
    this.minSimilarity = minSimilarity;
    this.env = env;
  }

  public SimSort(Double minSimilarity, ExecutionEnvironment env) {
    this(true, minSimilarity, env);
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

    graph = graph.run(new SimSortVertexCentricIteration(minSimilarity, env));

    graph = Graph.fromDataSet(graph.getVertices().map(x -> {
      LOG.info(x);
      return x;
    }).returns(new TypeHint<Vertex<Long, ObjectMap>>() {}), graph.getEdges(), env);



    return graph;
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
