package org.mappinganalysis.model.functions.decomposition;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.Preprocessing;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Methods needed for (initial) clustering.
 */
public class Clustering {
  private static final Logger LOG = Logger.getLogger(Clustering.class);


  /**
   * Connected components and minor refinement steps.
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, ObjectMap> createInitialClustering(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExampleOutput out, ExecutionEnvironment env) throws Exception {

    graph = Clustering.computeTransitiveClosureEdgeSimilarities(graph, env);
    graph = Clustering.removeOneToManyVertices(graph, env);
    out.addPreClusterSizes("2 intial cluster sizes", graph.getVertices(), Constants.CC_ID);

    return graph;
  }

  /**
   * Create edges from transitive closure and compute edge similarity
   *
   * 1. Compute transitive closure for a given graph and add the computed edges to the graph.
   * Direction of edges may change, two vertices have exactly one edge.
   * 2. For each of the computed edges, we compute a similarity value based on
   * label and geo coorodinates.
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, ObjectMap> computeTransitiveClosureEdgeSimilarities(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) throws Exception {

    graph = GraphUtils.addCcIdsToGraph(graph, env);
    Utils.writeGraphToJSONFile(graph, Constants.INIT_CLUST);

    final DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
        .getTransitiveClosureEdges(graph.getVertices(), new CcIdKeySelector());
    final DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
        .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
            Constants.SIM_GEO_LABEL_STRATEGY);
    graph = Graph.fromDataSet(graph.getVertices(), simEdges, env);
    return graph;
  }

  /**
   * After simple 1:n eliminating, still 1:n can reoccur after creating transitive closure
   * in components. The best candidate of the 1:n vertices remains in the vertex dataset,
   * others are removed.
   * // TODO refactor
   */
  public static Graph<Long, ObjectMap, ObjectMap> removeOneToManyVertices(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env) {
    DataSet<Tuple3<Long, String, Double>> oneToManyCandidates = graph
        .groupReduceOnNeighbors(
        new NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
            Tuple3<Long, String, Double>>() {
          @Override
          public void iterateNeighbors(
              Vertex<Long, ObjectMap> vertex,
              Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighborEdgeVertices,
              Collector<Tuple3<Long, String, Double>> out) throws Exception {
            String ontology = vertex.getValue().getOntology();
            int neighborCount = 0;
            double vertexAggSim = 0d;
            boolean isRelevant = false;

            for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> edgeVertex : neighborEdgeVertices) {
              Edge<Long, ObjectMap> edge = edgeVertex.f0;
              Vertex<Long, ObjectMap> neighbor = edgeVertex.f1;
              ++neighborCount;
              if (!isRelevant && neighbor.getValue().getOntology().equals(ontology)) {
                isRelevant = true;
              }
              vertexAggSim += edge.getValue().getEdgeSimilarity();
            }

            if (isRelevant) {
              vertexAggSim /= neighborCount;
              out.collect(new Tuple3<>(vertex.getId(), ontology, vertexAggSim));
            }
          }
        }, EdgeDirection.ALL);

    DataSet<Vertex<Long, ObjectMap>> bestCandidates = oneToManyCandidates.groupBy(1)
        .max(2).andMin(0)
        .map(tuple -> new Tuple2<>(tuple.f1, tuple.f2)) // string double
        .returns(new TypeHint<Tuple2<String, Double>>() {
        })
        .leftOuterJoin(oneToManyCandidates)
        .where(0, 1)
        .equalTo(1, 2)
        .with(new FlatJoinFunction<Tuple2<String, Double>,
            Tuple3<Long, String, Double>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<String, Double> left,
                           Tuple3<Long, String, Double> right,
                           Collector<Tuple1<Long>> out) throws Exception {
            if (left != null) {
              out.collect(new Tuple1<>(right.f0));
            }
          }
        })
        .leftOuterJoin(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple1<Long>,
            Vertex<Long, ObjectMap>,
            Vertex<Long, ObjectMap>>() {
          @Override
          public void join(Tuple1<Long> left,
                           Vertex<Long, ObjectMap> right,
                           Collector<Vertex<Long, ObjectMap>> out) throws Exception {
            if (left != null) {
              LOG.info("vertex best candidate: " + right.toString());
              out.collect(right);
            }
          }
        });

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices()
        .leftOuterJoin(oneToManyCandidates)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Vertex<Long, ObjectMap>,
            Tuple3<Long, String, Double>,
            Vertex<Long, ObjectMap>>() {
          @Override
          public void join(Vertex<Long, ObjectMap> left,
                           Tuple3<Long, String, Double> right,
                           Collector<Vertex<Long, ObjectMap>> out) throws Exception {
            if (right == null) {
              out.collect(left);
            }
          }
        })
        .union(bestCandidates);

    DataSet<Edge<Long, ObjectMap>> resultEdges = Preprocessing.deleteEdgesWithoutSourceOrTarget(
        graph.getEdges(),
        resultVertices);

    return Graph.fromDataSet(resultVertices, resultEdges, env);
  }
}
