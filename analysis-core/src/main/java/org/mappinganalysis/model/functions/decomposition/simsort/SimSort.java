package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.HashCcIdKeySelector;

public class SimSort {
  private static final Logger LOG = Logger.getLogger(SimSort.class);

  /**
   * create all missing edges, addGraph default vertex sim values
   * @param graph input graph
   * @param env execution environment
   * @return preprocessed graph
   */
  public static Graph<Long, ObjectMap, ObjectMap> prepare(Graph<Long, ObjectMap, ObjectMap> graph,
                                                          ExecutionEnvironment env) throws Exception {
    DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
        .getTransitiveClosureEdges(graph.getVertices(), new HashCcIdKeySelector());

    DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
        .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
            Constants.SIM_GEO_LABEL_STRATEGY);

    return Graph.fromDataSet(graph.getVertices(), simEdges, env);
  }

  /**
   * Execute SimSort procedure based on vertex-centric-iteration
   * @param maxIterations max vertex-centric-iteration count
   */
  public static Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph,
                                                          Integer maxIterations,
                                                          ExecutionEnvironment env) {
    VertexCentricConfiguration aggParameters = new VertexCentricConfiguration();
    aggParameters.setName("SimSort");
    aggParameters.setDirection(EdgeDirection.ALL);
    /**
     * set solution set unmanaged in order to reduce out of memory exception on non-cluster setup
     */
    aggParameters.setSolutionSetUnmanagedMemory(true);

    DataSet<Vertex<Long, SimSortVertexTuple>> workingVertices = createSimSortInputGraph(graph, env)
        .runVertexCentricIteration(
            new SimSortOptVertexUpdateFunction(Constants.MIN_SIMSORT_SIM),
            new SimSortOptMessagingFunction(), maxIterations, aggParameters)
        .getVertices();

    DataSet<Vertex<Long, ObjectMap>> resultingVertices = graph
        .getVertices()
        .join(workingVertices)
        .where(0)
        .equalTo(0)
        .with((vertex, workingVertex) -> {
//          LOG.info("v: " + vertex.toString() + " wv: " + workingVertex.toString());
          vertex.getValue().setHashCcId(workingVertex.getValue().getHash());
          if (workingVertex.getValue().getOldHash() != Long.MIN_VALUE) {
            vertex.getValue().setOldHashCcId(workingVertex.getValue().getOldHash());
          }
          vertex.getValue().setVertexStatus(workingVertex.getValue().isActive());

          return vertex;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    return Graph.fromDataSet(resultingVertices, graph.getEdges(), env);
  }

  /**
   * Converts input graph to SimSort input format using TupleX instead of complex ObjectMap.
   */
  private static Graph<Long, SimSortVertexTuple, SimSortEdgeTuple> createSimSortInputGraph(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) {

    DataSet<Edge<Long, SimSortEdgeTuple>> edges = graph.getEdges()
        .map(edge -> new Edge<>(edge.getSource(),
              edge.getTarget(),
              new SimSortEdgeTuple(edge.getValue().getEdgeSimilarity())))
        .returns(new TypeHint<Edge<Long, SimSortEdgeTuple>>() {});

    DataSet<Vertex<Long, SimSortVertexTuple>> vertices = graph
        .getVertices()
        .map(vertex -> new Vertex<>(vertex.getId(),
              new SimSortVertexTuple(vertex.getValue().getHashCcId(),
                  Long.MIN_VALUE, // not safe to assume
                  -1D,
                  Boolean.TRUE)))
        .returns(new TypeHint<Vertex<Long, SimSortVertexTuple>>() {});

    return Graph.fromDataSet(vertices, edges, env);
  }

  /**
   * Alternative sim-based refinement algorithm based on searching for cluster partitioning
   * with good average cluster similarity in sub clusters.
   */
  public static Graph<Long, ObjectMap, ObjectMap> executeAlternative(
      Graph<Long, ObjectMap, ObjectMap> graph,
       ExecutionEnvironment env) {

    // TODO prepare is ok, perhaps delete property Constants.VERTEX_AGG_SIM_VALUE
    // TODO for alternative version, unneeded

    return graph;
  }
}
