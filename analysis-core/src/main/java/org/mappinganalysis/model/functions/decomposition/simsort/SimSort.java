package org.mappinganalysis.model.functions.decomposition.simsort;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.*;
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

    return Graph.fromDataSet(graph.getVertices(), simEdges, env);
//        .mapVertices(new MapFunction<Vertex<Long, ObjectMap>, ObjectMap>() { // do not use lambda
//          @Override
//          public ObjectMap map(Vertex<Long, ObjectMap> value) throws Exception {
//            value.getValue().put(Constants.VERTEX_AGG_SIM_VALUE, Constants.DEFAULT_VERTEX_SIM);
//            return value.getValue();
//          }
//        });
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
    aggParameters.setSolutionSetUnmanagedMemory(true);

//    Graph<Long, ObjectMap, ObjectMap> workingGraph = graph
//        .mapVertices(new MapFunction<Vertex<Long, ObjectMap>, ObjectMap>() {
//          @Override
//          public ObjectMap map(Vertex<Long, ObjectMap> vertex) throws Exception {
//            ObjectMap properties = vertex.getValue();
//            properties.remove(Constants.LABEL);
//            properties.remove(Constants.TYPE_INTERN);
//            properties.remove(Constants.ONTOLOGY);
//            properties.remove(Constants.LON);
//            properties.remove(Constants.LAT);
//            properties.remove(Constants.CC_ID);
//
//            return properties;
//          }
//        });

    // TODO SimSortOptimized: remove all strings and use tuples only

    // TODO workaround to test upper boundaries for "setSolutionSetUnmanagedMemory" false: 700k working, 800 not anymore

    DataSet<Vertex<Long, ObjectMap>> preVertices = graph.getVertices();
//        .first(1400000);

    DataSet<Edge<Long, SimSortEdgeTuple>> inputEdges = graph.getEdges()
//        Preprocessing
//        .deleteEdgesWithoutSourceOrTarget(graph.getEdges(), preVertices)
        .map(edge -> new Edge<>(edge.getSource(),
            edge.getTarget(),
            new SimSortEdgeTuple(edge.getValue().getEdgeSimilarity())))
        .returns(new TypeHint<Edge<Long, SimSortEdgeTuple>>() {});

    DataSet<Vertex<Long, SimSortVertexTuple>> inputVertices = preVertices
        .map(vertex -> new Vertex<>(vertex.getId(),
            new SimSortVertexTuple(vertex.getValue().getCcId(),
                Long.MIN_VALUE, // not safe to assume
                -1D,
                Boolean.TRUE)))
        .returns(new TypeHint<Vertex<Long, SimSortVertexTuple>>() {});


//    workingGraph = Graph.fromDataSet(wVertices,
//        Preprocessing.deleteEdgesWithoutSourceOrTarget(graph.getEdges(), wVertices),
//        env);

    Graph<Long, SimSortVertexTuple, SimSortEdgeTuple> inputGraph
        = Graph.fromDataSet(inputVertices, inputEdges, env);

    DataSet<Vertex<Long, SimSortVertexTuple>> workingVertices = inputGraph
        .runVertexCentricIteration(
            new SimSortOptVertexUpdateFunction(Constants.MIN_SIMSORT_SIM),
            new SimSortOptMessagingFunction(), maxIterations, aggParameters)
        .getVertices();

    // todo old version
//    DataSet<Vertex<Long, ObjectMap>> workingVertices = workingGraph
//        .runVertexCentricIteration(
//            new SimSortVertexUpdateFunction(Constants.MIN_SIMSORT_SIM), // TODO set default sim value!
//            new SimSortMessagingFunction(), maxIterations, aggParameters)
//        .getVertices();

    DataSet<Vertex<Long, ObjectMap>> resultingVertices = graph
        .getVertices()
        .join(workingVertices)
        .where(0)
        .equalTo(0)
        .with((vertex, workingVertex) -> {
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

    // todo map vertices remove checked property from filter

    return Graph.fromDataSet(graph.getVertices(),
        componentGraph.getEdges(), env);
  }
}
