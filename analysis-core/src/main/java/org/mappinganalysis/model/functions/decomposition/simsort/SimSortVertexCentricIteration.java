package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.util.Constants;

/**
 * Execute SimSort procedure based on vertex-centric-iteration
 */
public class SimSortVertexCentricIteration
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, Graph<Long, ObjectMap, ObjectMap>> {
  private final ExecutionEnvironment env;

  public SimSortVertexCentricIteration(ExecutionEnvironment env) {
    this.env = env;
  }

  @Override
  public Graph<Long, ObjectMap, ObjectMap> run(Graph<Long, ObjectMap, ObjectMap> graph) throws Exception {
    VertexCentricConfiguration aggParameters = new VertexCentricConfiguration();
    aggParameters.setName("SimSort");
    aggParameters.setDirection(EdgeDirection.ALL);
    /**
     * set solution set unmanaged in order to reduce out of memory exception on non-cluster setup
     */
//    aggParameters.setSolutionSetUnmanagedMemory(true);

    DataSet<Vertex<Long, SimSortVertexTuple>> workingVertices = createSimSortInputGraph(graph, env)
        .runVertexCentricIteration(
            new SimSortOptVertexUpdateFunction(Constants.MIN_SIMSORT_SIM),
            new SimSortOptMessagingFunction(), Integer.MAX_VALUE, aggParameters)
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
}
