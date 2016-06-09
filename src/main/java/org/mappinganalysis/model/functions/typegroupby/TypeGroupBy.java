package org.mappinganalysis.model.functions.typegroupby;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
    graph = GraphUtils.addCcIdsToGraph(graph);
    DataSet<Vertex<Long, ObjectMap>> vertices = graph.getVertices()
        .map(new AddShadingTypeMapFunction())
        .filter(vertex -> {
          LOG.info("shadingVertex: " + vertex.toString());
          return true;
        })
        .groupBy(new CcIdAndCompTypeKeySelector())
        .reduceGroup(new GenerateHashCcIdGroupReduceFunction());

    graph = Graph.fromDataSet(vertices, graph.getEdges(), env);

    out.addVertexAndEdgeSizes("2 vertex and edge sizes post preprocessing", graph);
    out.addPreClusterSizes("2 cluster sizes post preprocessing", graph.getVertices(), Utils.CC_ID);
    Utils.writeToHdfs(graph.getVertices(), "2_post_preprocessing");


//    processingMode = Utils.PROCESSING_MODE_NEXT_TGB;
    LOG.info("processingMode: " + processingMode);
    if (processingMode.equals(Utils.PROCESSING_MODE_NEXT_TGB)) {
      // TODO new tgb code
      DataSet<Edge<Long, NullValue>> distinctEdges = GraphUtils
          .getTransitiveClosureEdges(graph.getVertices(), new CcIdKeySelector());
      DataSet<Edge<Long, ObjectMap>> simEdges = SimilarityComputation
          .computeGraphEdgeSim(Graph.fromDataSet(graph.getVertices(), distinctEdges, env),
              Utils.SIM_GEO_LABEL_STRATEGY);

      simEdges = simEdges.filter(value -> {
        LOG.info("distinctEdge: " + value.toString());
        return true;
      });
      graph = Graph.fromDataSet(vertices, simEdges, env);

      out.addDataSetCount("newVertices count", vertices);
    }

    //different type group by implementations
    // TODO change preprocessing more edges

//TODO new stuff not working at all
//    DataSet<Tuple4<Long, Double, String, Long>> neighborSimTypes = graph
//        .groupReduceOnNeighbors(new NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
//            Tuple4<Long, Double, String, Long>>() {
//          @Override
//          public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
//                                       Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
//                                       Collector<Tuple4<Long, Double, String, Long>> out) throws Exception {
//            if (vertex.getValue().get(Utils.TYPE_INTERN).toString().equals(Utils.NO_TYPE)) {
//              for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
//                LOG.info("neighborVals: " + vertex.getId() + " " + neighbor.f0.getValue().getSimilarity()
//                    + " " + neighbor.f1.getValue().get(Utils.TYPE_INTERN).toString());
//                out.collect(new Tuple4<>(vertex.getId(),
//                    neighbor.f0.getValue().getSimilarity(),
//                    neighbor.f1.getValue().get(Utils.TYPE_INTERN).toString(),
//                    neighbor.f1.getValue().getHashCcId()));
//              }
////                neighbors.forEach(neighbor -> out.collect(new Tuple4<>(vertex.getId(),
////                    neighbor.f0.getValue().getSimilarity(),
////                    neighbor.f1.getValue().get(Utils.TYPE_INTERN).toString(),
////                    neighbor.f1.getValue().getHashCcId())));
//            }
//          }
//        }, EdgeDirection.ALL);
//
//
//    DataSet<Vertex<Long, ObjectMap>> tmpVerts = graph.getVertices().leftOuterJoin(neighborSimTypes)
//        .where(0)
//        .equalTo(0)
//        .with((left, right) -> left)
//        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
//
//    graph = Graph.fromDataSet(tmpVerts, graph.getEdges(), env);

    if (processingMode.equals(Utils.PROCESSING_MODE_NEXT_TGB)) {

      LOG.info("newTgbMethod");

      DataSet<Tuple4<Long, Double, String, Long>> neighborSimTypes = graph
          .groupReduceOnNeighbors(new NeighborsFunctionWithVertexValue<Long, ObjectMap, ObjectMap,
              Tuple4<Long, Double, String, Long>>() {
            @Override
            public void iterateNeighbors(Vertex<Long, ObjectMap> vertex,
                                         Iterable<Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>>> neighbors,
                                         Collector<Tuple4<Long, Double, String, Long>> out) throws Exception {
              if (vertex.getValue().get(Utils.TYPE_INTERN).toString().equals(Utils.NO_TYPE)) {
                for (Tuple2<Edge<Long, ObjectMap>, Vertex<Long, ObjectMap>> neighbor : neighbors) {
                  LOG.info("neighborVals: " + vertex.getId() + " " + neighbor.f0.getValue().getSimilarity()
                  + " " + neighbor.f1.getValue().get(Utils.TYPE_INTERN).toString());
                  out.collect(new Tuple4<>(vertex.getId(),
                      neighbor.f0.getValue().getSimilarity(),
                      neighbor.f1.getValue().get(Utils.TYPE_INTERN).toString(),
                      neighbor.f1.getValue().getHashCcId()));
                }
//                neighbors.forEach(neighbor -> out.collect(new Tuple4<>(vertex.getId(),
//                    neighbor.f0.getValue().getSimilarity(),
//                    neighbor.f1.getValue().get(Utils.TYPE_INTERN).toString(),
//                    neighbor.f1.getValue().getHashCcId())));
              }
            }
          }, EdgeDirection.ALL);

//      DataSet<Tuple4<Long, Double, String, Long>> maxVertexSimValues = neighborSimTypes
//          .filter(value -> !value.f2.equals(Utils.NO_TYPE))
//          .groupBy(0)
//          .max(1).andMin(3);
//
//      DataSet<Tuple4<Long, Double, String, Long>> maxVertSimTypes = maxVertexSimValues
//          .leftOuterJoin(neighborSimTypes)
//          .where(0)
//          .equalTo(0)
//          .with((left, right) -> {
//            LOG.info("maxVertSimTypes: " + right);
//            return right;})
//          .returns(new TypeHint<Tuple4<Long, Double, String, Long>>() {
//          });
//
//      DataSet<Tuple4<Long, Double, String, Long>> noTypeCandidates = maxVertSimTypes
//          .rightOuterJoin(neighborSimTypes)
//          .where(0)
//          .equalTo(0)
//          .with(new FlatJoinFunction<Tuple4<Long, Double, String, Long>, Tuple4<Long, Double, String, Long>, Tuple4<Long, Double, String, Long>>() {
//            @Override
//            public void join(Tuple4<Long, Double, String, Long> first,
//                             Tuple4<Long, Double, String, Long> second,
//                             Collector<Tuple4<Long, Double, String, Long>> out) throws Exception {
//              if (first == null) {
//                LOG.info("noTypeCandidate: " + second.toString());
//                out.collect(second);
//              }
//            }
//          });
//
//      DataSet<Tuple4<Long, Double, String, Long>> minNoTypeCcs = noTypeCandidates
//          .groupBy(0)
//          .min(3);
//
//      DataSet<Tuple4<Long, Double, String, Long>> noTypeVertices = noTypeCandidates
//          .distinct(0)
//          .leftOuterJoin(minNoTypeCcs)
//          .where(0)
//          .equalTo(0)
//          .with((left, right) -> new Tuple4<>(left.f0, left.f1, left.f2, right.f3))
//          .returns(new TypeHint<Tuple4<Long, Double, String, Long>>() {});
//
//      DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
//          .leftOuterJoin(noTypeVertices.union(maxVertSimTypes))
//          .where(0)
//          .equalTo(0)
//          .with((left, right) -> {
//            left.getValue().put(Utils.HASH_CC, right.f3);
//            return left;
//          })
//          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});
//
//      out.addDataSetCount("newVertices count", newVertices);

      DataSet<Vertex<Long, ObjectMap>> verts = graph.getVertices().join(neighborSimTypes)
          .where(0)
          .equalTo(0)
          .with((left, right) -> left)
          .returns(new TypeHint<Vertex<Long, ObjectMap>>() {
          });


      return Graph.fromDataSet(verts, graph.getEdges(), env);
    } else if (processingMode.equals(Utils.DEFAULT_VALUE)) { // old vertex centric iteration
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
}
