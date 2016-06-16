package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.*;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.preprocessing.GenerateHashCcIdGroupReduceFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.utils.Utils;
import org.mappinganalysis.utils.functions.keyselector.CcIdAndCompTypeKeySelector;
import org.mappinganalysis.utils.functions.keyselector.CcIdKeySelector;

import java.util.Optional;
import java.util.Set;

public class TypeGroupBy {
  private static final Logger LOG = Logger.getLogger(TypeGroupBy.class);

  /**
   * For a given graph, assign all vertices with no type to the component where the best similarity can be found.
   * @param graph input graph
   * @param processingMode if default, typeGroupBy is executed
   * @param maxIterations maximal count vertex centric iterations
   * @return graph where non-type vertices are assigned to best matching component
   */
  public static Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, ObjectMap> graph,
                                                   String processingMode,
                                                   Integer maxIterations,
                                                   ExecutionEnvironment env, ExampleOutput out) throws Exception {
    // type groupby preprocessing
    graph = GraphUtils.addCcIdsToGraph(graph, env);

    DataSet<Vertex<Long, ObjectMap>> tmpVertices = graph.getVertices().filter(value -> true); // sync needed (only sometimes?)
    DataSet<Edge<Long, ObjectMap>> edges = graph.getEdges().filter(value -> true);
    graph = Graph.fromDataSet(tmpVertices, edges, env);

    final DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices()
        .map(new AddShadingTypeMapFunction())
        .filter(vertex -> {
          LOG.info("shadingVertex: " + vertex.toString());
          return true;
        })
        .groupBy(new CcIdAndCompTypeKeySelector())
        .reduceGroup(new GenerateHashCcIdGroupReduceFunction());

    graph = Graph.fromDataSet(vertices, graph.getEdges(), env);
    // end preprocessing
    LOG.info("mode: " + Utils.IS_TGB_DEFAULT_MODE);

    if (!Utils.IS_TGB_DEFAULT_MODE) {
      final DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
          .getTransitiveClosureEdges(graph.getVertices(), new CcIdKeySelector());
      final DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
          .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
              Utils.SIM_GEO_LABEL_STRATEGY);

      graph = Graph.fromDataSet(graph.getVertices(), simEdges, env);

      out.addVertexAndEdgeSizes("2 vertex and edge sizes after preprocessing", graph);
      out.addPreClusterSizes("2 cluster sizes after preprocessing", graph.getVertices(), Utils.CC_ID);
//      Utils.writeToHdfs(graph.getVertices(), "2_post_preprocessing");
      out.print();

      final DataSet<Tuple4<Long, Double, Set<String>, Long>> neighborSimTypes = graph
          .groupReduceOnNeighbors(new NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
              Tuple4<Long, Double, Set<String>, Long>>() {
            @Override
            public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
                                         Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
                                         Collector<Tuple4<Long, Double, Set<String>, Long>> out) throws Exception {
              String vertexType = vertex.getValue().getTypes(Utils.TYPE_INTERN).stream().findFirst().get();
              if (vertexType.equals(Utils.NO_TYPE)) {
                neighbors.forEach(neighbor -> out.collect(new Tuple4<>(vertex.getId(),
                    neighbor.f0.getValue().getSimilarity(),
                    neighbor.f1.getValue().getTypes(Utils.TYPE_INTERN),
                    neighbor.f1.getValue().getHashCcId())));
              }
            }
          }, EdgeDirection.ALL);

      final DataSet<Tuple4<Long, Double, Set<String>, Long>> maxTypedSimValues = getMaxNeighborSims(neighborSimTypes);

      // all tuples minus max tuple ids with type
      DataSet<Tuple4<Long, Double, Set<String>, Long>> noTypedNeighborsCandidates = neighborSimTypes
          .leftOuterJoin(maxTypedSimValues)
          .where(0)
          .equalTo(0)
          .with(new FlatJoinFunction<Tuple4<Long, Double, Set<String>, Long>,
              Tuple4<Long, Double, Set<String>, Long>, Tuple4<Long, Double, Set<String>, Long>>() {
            @Override
            public void join(Tuple4<Long, Double, Set<String>, Long> first,
                             Tuple4<Long, Double, Set<String>, Long> second,
                             Collector<Tuple4<Long, Double, Set<String>, Long>> out) throws Exception {
              if (second == null) {
//                LOG.info("noTypeCandidate: " + first.toString());
                out.collect(first);
              }
            }
          });

//      // TODO tmp log begin
//      DataSet<Vertex<Long, ObjectMap>> newVertices = vertices.leftOuterJoin(tmp1)
//          .where(0)
//          .equalTo(0)
//          .with((left, right) -> {
//            if (right != null)
//              LOG.info("right: " + right.toString());
//            return left;
//          })
//          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {
//          });
//
//      graph = Graph.fromDataSet(newVertices, graph.getEdges(), env);
//      // TODO tmp log end

      DataSet<Vertex<Long, ObjectMap>> noTypedNeighbors = noTypedNeighborsCandidates
          .groupBy(0)
          .min(3)
          .leftOuterJoin(vertices)
          .where(0)
          .equalTo(0)
          .with((left, right) -> {
            if (right.getValue().getHashCcId() < left.f3) {
            right.getValue().put(Utils.HASH_CC, left.f3);

            LOG.info("right:111 " + right.toString());
            return right;
            } else {
              LOG.info("right:222 " + right.toString());
              return right;
            }
          })
          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

      DataSet<Vertex<Long, ObjectMap>> typedNeighbors = maxTypedSimValues
          .leftOuterJoin(graph.getVertices())
          .where(0)
          .equalTo(0)
          .with((left, right) -> {
            LOG.info("right:typed " + right.toString());

            right.getValue().put(Utils.HASH_CC, left.f3);
            return right;
          })
          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {
          });

//      // TODO tmp log begin
      graph = Graph.fromDataSet(noTypedNeighbors.union(typedNeighbors), graph.getEdges(), env);
//      // TODO tmp log end

//      DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
//          .leftOuterJoin(noTypedNeighbors.union(typedNeighbors))
//          .where(0)
//          .equalTo(0)
//          .with((unchanged, updated) -> {
//            if (updated == null) {
//              LOG.info("final: unchanged: " + unchanged.toString());
//              return unchanged;
//            } else {
//              LOG.info("final: unchanged: " + unchanged.toString() + " updated: " + updated.toString());
//              return updated;
//            }
//          })
//          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
//
//      graph = Graph.fromDataSet(newVertices, graph.getEdges(), env);

      return graph;
      // check also simcomp code TODO
    } else if (Utils.IS_TGB_DEFAULT_MODE) { // old vertex centric iteration
      VertexCentricConfiguration tbcParams = new VertexCentricConfiguration();
      tbcParams.setName("Type-based Cluster Generation Iteration");
      tbcParams.setDirection(EdgeDirection.ALL);

      graph = graph.runVertexCentricIteration(
          new TypeGroupByVertexUpdateFunction(),
          new TypeGroupByMessagingFunction(), maxIterations, tbcParams);

      return graph;
    } else {
      return graph;
    }
  }

  private static DataSet<Tuple4<Long, Double, Set<String>, Long>> getMaxNeighborSims(
      DataSet<Tuple4<Long, Double, Set<String>, Long>> neighborSimTypes) {

    final DataSet<Tuple4<Long, Double, Set<String>, Long>> typeVals = neighborSimTypes
        .filter(value -> !value.f2.contains(Utils.NO_TYPE));

    return typeVals
            .groupBy(0).max(1)
            .leftOuterJoin(typeVals)
            .where(0,1)
            .equalTo(0,1)
            .with((left, right) -> right)
            .returns(new TypeHint<Tuple4<Long, Double, Set<String>, Long>>() {})
            .groupBy(0)
            .min(3);
  }
}
