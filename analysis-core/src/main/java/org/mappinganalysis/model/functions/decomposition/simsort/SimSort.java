package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.DataDomain;
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
  private DataDomain domain;

  /**
   * check constructors TODO null is never good here
   */
  public SimSort(Boolean prepareEnabled, Double minSimilarity, ExecutionEnvironment env) {
    this(null, prepareEnabled, minSimilarity, env);
  }

  /**
   * Basic Constructor
   */
  private SimSort(DataDomain domain, Boolean prepareEnabled, Double minSimilarity, ExecutionEnvironment env) {
    this.domain = domain;
    this.prepareEnabled = prepareEnabled;
    this.minSimilarity = minSimilarity;
    this.env = env;
  }

  /**
   * Constructor - prepareEnabled true
   */
  public SimSort(Double minSimilarity, ExecutionEnvironment env) {
    this(true, minSimilarity, env);
  }

  /**
   * Constructor - provide a DataDomain (geo or music) and min similarity, prepareEnabled true
   */
  public SimSort(DataDomain domain, Double minSimilarity, ExecutionEnvironment env) {
    this(domain, true, minSimilarity, env);
  }

  /**
   * Execute SimSort procedure based on vertex-centric-iteration, as preparation:
   * create all missing edges, addGraph default vertex sim values
   */
  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(
      Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
    String mode = null;
    if (domain == DataDomain.MUSIC) {
      mode = Constants.MUSIC;
    } else if (domain == DataDomain.GEOGRAPHY) {
      mode = Constants.GEO;
    } else if (domain == DataDomain.NC) {
      mode = Constants.NC;
    }

    /**
     * TODO THIS IS THE PROBLEM
     */
    if (prepareEnabled) {
      DataSet<Edge<Long, NullValue>> distinctEdges = graph
          .getVertices()
          .runOperation(new EdgeComputationVertexCcSet(new HashCcIdKeySelector()));

      graph = Graph
          .fromDataSet(graph.getVertices(), distinctEdges, env)
          .run(new BasicEdgeSimilarityComputation(mode, env));
    }

    graph = graph
        .run(new SimSortVertexCentricIteration(minSimilarity, env));

    return Graph.fromDataSet(graph.getVertices(), graph.getEdges(), env);
  }

  /**
   * create all missing edges, addGraph default vertex sim values
   */
  @Deprecated
  public static Graph<Long, ObjectMap, ObjectMap> prepare(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) throws Exception {
    DataSet<Edge<Long, NullValue>> distinctEdges = graph.getVertices()
        .runOperation(new EdgeComputationVertexCcSet(new HashCcIdKeySelector()));

    return Graph.fromDataSet(graph.getVertices(), distinctEdges, env)
        .run(new BasicEdgeSimilarityComputation(Constants.GEO, env));
  }
}
