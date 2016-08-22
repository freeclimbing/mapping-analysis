package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.io.output.ExampleOutput;
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
                                                          ExecutionEnvironment env,
                                                          ExampleOutput out) throws Exception {
    DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
        .getTransitiveClosureEdges(graph.getVertices(), new HashCcIdKeySelector());

    DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
        .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
            Constants.SIM_GEO_LABEL_STRATEGY);

    return Graph.fromDataSet(graph.getVertices(), simEdges, env)
        .mapVertices(new MapFunction<Vertex<Long, ObjectMap>, ObjectMap>() { // do not use lambda
          @Override
          public ObjectMap map(Vertex<Long, ObjectMap> value) throws Exception {
            value.getValue().put(Constants.VERTEX_AGG_SIM_VALUE, Constants.DEFAULT_VERTEX_SIM);
            return value.getValue();
          }
        });
  }

  /**
   * Execute SimSort procedure based on vertex-centric-iteration
   * @param maxIterations max vertex-centric-iteration count
   */
  public static Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph,
                                                          Integer maxIterations) {
    VertexCentricConfiguration aggParameters = new VertexCentricConfiguration();
    aggParameters.setName("SimSort");
    aggParameters.setDirection(EdgeDirection.ALL);

    return graph.runVertexCentricIteration(
        new SimSortVertexUpdateFunction(Constants.MIN_SIMSORT_SIM),
        new SimSortMessagingFunction(), maxIterations, aggParameters);
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

  /**
   * SimSort post processing exclude low sim vertices
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, ObjectMap> excludeLowSimVertices(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) throws Exception {
    // only edges are interesting
    Graph<Long, ObjectMap, ObjectMap> componentGraph = graph
        .filterOnVertices(new SimSortExcludeLowSimFilterFunction(true));

    return Graph.fromDataSet(graph.getVertices(),
        componentGraph.getEdges(), env);
  }
}
