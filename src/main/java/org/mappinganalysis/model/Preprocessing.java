package org.mappinganalysis.model;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.GraphUtils;
import org.mappinganalysis.io.DataLoader;
import org.mappinganalysis.io.functions.EdgeRestrictFlatJoinFunction;
import org.mappinganalysis.io.functions.VertexRestrictFlatJoinFunction;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.functions.preprocessing.*;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.utils.Stats;
import org.mappinganalysis.utils.Utils;
import org.mappinganalysis.utils.functions.keyselector.CcIdAndCompTypeKeySelector;
import org.mappinganalysis.utils.functions.keyselector.CcIdKeySelector;

/**
 * Preprocessing.
 */
public class Preprocessing {
  private static final Logger LOG = Logger.getLogger(Preprocessing.class);

  /**
   * Execute all preprocessing steps with the given options
   * @param graph input graph
   * @param env execution environment  @return graph   @throws Exception
   */
  public static Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, NullValue> graph,
                                                          ExecutionEnvironment env,
                                                          ExampleOutput out) throws Exception {
    graph = applyTypeToInternalTypeMapping(graph, env);
    graph = GraphUtils.addCcIdsToGraph(graph, env, 1);

//    Utils.writeToHdfs(graph.getVertices(), "1_input_graph_withCc");
//    out.addPreClusterSizes("1 cluster sizes input graph", graph.getVertices(), Utils.CC_ID);

    if (Utils.IS_RESTRICT_ACTIVE) {
      graph = restrictGraph(graph, env);
    }

    graph = applyTypeMissMatchCorrection(graph, true, env);
    Graph<Long, ObjectMap, ObjectMap> simGraph = Graph.fromDataSet(
        graph.getVertices(),
        SimilarityComputation.computeGraphEdgeSim(graph, Utils.DEFAULT_VALUE),
        env);

    return applyLinkFilterStrategy(simGraph, env, Utils.IS_LINK_FILTER_ACTIVE);
  }

  private static Graph<Long, ObjectMap, NullValue> restrictGraph(Graph<Long, ObjectMap, NullValue> graph,
                                                                 ExecutionEnvironment env) {
    // restrict to first 100k clusters
    DataSet<Tuple1<Long>> restrictedComponentIds = graph.getVertices()
        .map(vertex -> new Tuple1<>((long) vertex.getValue().get(Utils.CC_ID)))
        .returns(new TypeHint<Tuple1<Long>>() {})
//        .filter(tuple -> {
//          return tuple.f0 == 1868L;
////            return tuple.f0 == 1134L || tuple.f0 == 60L;// || tuple.f0 == 1135L || tuple.f0 == 8214L; // typegroupby diff
////            return tuple.f0 == 890L || tuple.f0 == 1134L || tuple.f0 == 60L || tuple.f0 == 339L; // typegroupby diff
//        });
        .first(500);

    DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
        .map(vertex -> new Tuple2<>(vertex.getId(), (long) vertex.getValue().get(Utils.CC_ID))) //vid, ccid
        .returns(new TypeHint<Tuple2<Long, Long>>() {})
        .join(restrictedComponentIds)
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple1<Long>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<Long, Long> left, Tuple1<Long> right, Collector<Tuple1<Long>> collector)
              throws Exception {
            collector.collect(new Tuple1<>(left.f0));
          }
        })
        .leftOuterJoin(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with((longTuple1, vertex) -> vertex).returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Edge<Long, NullValue>> newEdges = deleteEdgesWithoutSourceOrTarget(graph.getEdges(), newVertices);

    graph = Graph.fromDataSet(newVertices.distinct(0), newEdges.distinct(0,1), env);
    return graph;
  }

  /**
   * CSV Reader todo fix duplicate code
   * @return graph with vertices and edges.
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, NullValue> getInputGraphFromCsv(ExecutionEnvironment env)
      throws Exception {
    DataLoader loader = new DataLoader(env);
    final String vertexFile = "concept.csv";
    final String edgeFile = "linksWithIDs.csv";
    final String propertyFile = "concept_attributes.csv";
    final String ccFile = "cc.csv";

    DataSet<Vertex<Long, ObjectMap>> vertices = loader
        .getVerticesFromCsv(Utils.INPUT_DIR + vertexFile, Utils.INPUT_DIR + propertyFile);

    if (Utils.INPUT_DIR.contains("linklion")) {
//      vertices = getNytVerticesLinklion(env, ccFile, vertices);
      vertices = getRandomCcsFromLinklion(env, ccFile, vertices);
    }

    DataSet<Edge<Long, NullValue>> edges
        = deleteEdgesWithoutSourceOrTarget(loader.getEdgesFromCsv(Utils.INPUT_DIR + edgeFile), vertices);

    vertices = deleteVerticesWithoutAnyEdges(vertices, edges.<Tuple2<Long, Long>>project(0, 1));
//    out.addDataSetCount("relevant vertices", vertices);
//
//    edges = deleteEdgesWithoutSourceOrTarget(edges, vertices);
//    out.addDataSetCount("second edge count", edges);

//    out.print();

    return Graph.fromDataSet(
        vertices,
        edges,
        env);
  }

  /**
   * Restrict LinkLion dataset by taking random CCs
   */
  private static DataSet<Vertex<Long, ObjectMap>> getRandomCcsFromLinklion(ExecutionEnvironment env,
                                                                           String ccFile,
                                                                           DataSet<Vertex<Long, ObjectMap>> vertices) {
    DataSet<Tuple2<Long, Long>> vertexIdAndCcs = getBaseVertexCcs(env, ccFile);

    DataSet<Tuple1<Long>> relevantCcs = vertexIdAndCcs.<Tuple1<Long>>project(1).first(1000);

    vertices = restrictVerticesToGivenCcs(vertices, vertexIdAndCcs, relevantCcs);

    return vertices;
  }

  private static DataSet<Vertex<Long, ObjectMap>> restrictVerticesToGivenCcs(DataSet<Vertex<Long, ObjectMap>> vertices,
                                                                             DataSet<Tuple2<Long, Long>> vertexCcs,
                                                                             DataSet<Tuple1<Long>> relevantCcs) {
    vertices = relevantCcs.join(vertexCcs)
        .where(0)
        .equalTo(1)
        .with((left, right) -> new Tuple1<>(right.f0))
        .returns(new TypeHint<Tuple1<Long>>() {})
        .join(vertices)
        .where(0)
        .equalTo(0)
        .with((id, vertex) -> {
          LOG.info("restrictedVertex: " + vertex);
          return vertex;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .distinct(0);
    return vertices;
  }

  private static DataSet<Tuple2<Long, Long>> getBaseVertexCcs(ExecutionEnvironment env, String ccFile) {
    return env.readCsvFile(Utils.INPUT_DIR + ccFile)
          .fieldDelimiter(";")
          .ignoreInvalidLines()
          .types(Integer.class, Integer.class)
          .map(value -> new Tuple2<>((long) value.f0, (long) value.f1))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});
  }

  /**
   * Restrict LinkLion dataset by (currently) taking all vertices from CCs
   * where nyt entities are contained (~6000 entities).
   *
   * Second option: take random ccs, WIP
   */
  private static DataSet<Vertex<Long, ObjectMap>> getNytVerticesLinklion(ExecutionEnvironment env,
                                                                         String ccFile,
                                                                         DataSet<Vertex<Long, ObjectMap>> vertices) {
    DataSet<Vertex<Long, ObjectMap>> nytFbVertices = vertices.filter(vertex ->
        vertex.getValue().get(Utils.ONTOLOGY).equals(Utils.NYT_NS));
//          && vertex.getValue().get(Utils.ONTOLOGY).equals(Utils.FB_NS));

    DataSet<Tuple2<Long, Long>> vertexCcs = getBaseVertexCcs(env, ccFile);

    DataSet<Tuple1<Long>> relevantCcs = vertexCcs.rightOuterJoin(nytFbVertices)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Vertex<Long, ObjectMap>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<Long, Long> first, Vertex<Long, ObjectMap> second, Collector<Tuple1<Long>> out) throws Exception {
//              LOG.info("inputVertexZero: " + first.toString() + " " + second.toString());
            out.collect(new Tuple1<>(first.f1));
          }
        });

//      out.addDataSetCount("relevant ccs nyt", relevantCcs);

    vertices = restrictVerticesToGivenCcs(vertices, vertexCcs, relevantCcs);

//      out.addDataSetCount("relevant vertices", vertices);
    return vertices;
  }

  /**
   * Delete edges where source or target vertex are not in the vertex set.
   */
  public static DataSet<Edge<Long, NullValue>> deleteEdgesWithoutSourceOrTarget(
      DataSet<Edge<Long, NullValue>> edges, DataSet<Vertex<Long, ObjectMap>> newVertices) {
    return edges.leftOuterJoin(newVertices)
        .where(0).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction())
        .leftOuterJoin(newVertices)
        .where(1).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction())
        .distinct();
  }

  /**
   * Delete vertices which are not source or target of an edge.
   * @param vertices (reduced) set of input vertices
   * @param edges corresponding edge set
   * @return vertices
   */
  public static DataSet<Vertex<Long, ObjectMap>> deleteVerticesWithoutAnyEdges(
      DataSet<Vertex<Long, ObjectMap>> vertices, DataSet<Tuple2<Long, Long>> edges) {

    DataSet<Vertex<Long, ObjectMap>> left = vertices
        .leftOuterJoin(edges)
        .where(0).equalTo(0)
        .with(new VertexRestrictFlatJoinFunction());

    return vertices
        .leftOuterJoin(edges)
        .where(0).equalTo(1)
        .with(new VertexRestrictFlatJoinFunction())
        .union(left)
        .distinct(0);
  }

  /**
   * Create the input graph for further analysis,
   * restrict to edges where source and target are in vertices set.
   * @return graph with vertices and edges.
   * @throws Exception
   * @param fullDbString complete server+port+db string
   * @deprecated load from db too slow
   */
  public static Graph<Long, ObjectMap, NullValue> getInputGraph(String fullDbString, ExecutionEnvironment env)
      throws Exception {
    DataLoader loader = new DataLoader(env);
    DataSet<Vertex<Long, ObjectMap>> vertices = loader.getVertices(fullDbString);

    // restrict edges to these where source and target are vertices
    DataSet<Edge<Long, NullValue>> edges = loader.getEdges(fullDbString)
        .leftOuterJoin(vertices)
        .where(0).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction())
        .leftOuterJoin(vertices)
        .where(1).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction());

    return Graph.fromDataSet(
        deleteVerticesWithoutAnyEdges(vertices, edges.<Tuple2<Long, Long>>project(0, 1)),
        edges,
        env);
  }

  /**
   * Preprocessing strategy to restrict resources to have only one counterpart in every target ontology.
   *
   * Strategy: delete all but best links which are involved in 1:n mappings
   * @param graph input graph
   * @param env environment
   * @param isLinkFilterActive boolean if filter should be used
   * @return output graph
   */
  public static Graph<Long, ObjectMap, ObjectMap> applyLinkFilterStrategy(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env,
      boolean isLinkFilterActive) throws Exception {

    if (isLinkFilterActive) {
      // Tuple7(edge src, edge trg, VertexId, Ontology, One, EdgeSim, VertexCc)
      final DataSet<Tuple6<Long, Long, Long, String, Integer, Double>> basicOneToManyTuples = graph
          .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.ALL);

      DataSet<Tuple3<Long, String, Double>> maxRandomTuples = basicOneToManyTuples
          .groupBy(2, 3)
          .sum(4).andMax(5)
          .filter(tuple -> tuple.f4 > 1)
          .map(tuple -> new Tuple3<>(tuple.f2, tuple.f3, tuple.f5))
          .returns(new TypeHint<Tuple3<Long, String, Double>>() {});

      DataSet<Edge<Long, ObjectMap>> maxOneToManyEdges = maxRandomTuples
          .leftOuterJoin(basicOneToManyTuples)
          .where(0, 1, 2)
          .equalTo(2, 3, 5)
          .with((first, second) -> new Tuple2<>(second.f0, second.f1))
          .returns(new TypeHint<Tuple2<Long, Long>>() {})
          .leftOuterJoin(graph.getEdges())
          .where(0, 1)
          .equalTo(0, 1)
          .with((first, second) -> second)
          .returns(new TypeHint<Edge<Long, ObjectMap>>() {});

      DataSet<Edge<Long, ObjectMap>> newEdges = graph.getEdges()
          .leftOuterJoin(maxRandomTuples.<Tuple1<Long>>project(0))
          .where(0)
          .equalTo(0)
          .with(new LinkFilterExcludeEdgeFlatJoinFunction())
          .leftOuterJoin(maxRandomTuples.<Tuple1<Long>>project(0))
          .where(1)
          .equalTo(0)
          .with(new LinkFilterExcludeEdgeFlatJoinFunction())
          .union(maxOneToManyEdges);

      DataSet<Vertex<Long, ObjectMap>> resultVertices = deleteVerticesWithoutAnyEdges(
          graph.getVertices(),
          newEdges.<Tuple2<Long, Long>>project(0, 1));

      return Graph.fromDataSet(resultVertices, newEdges, env);
    } else {
      return graph;
    }
  }

  /**
   * Harmonize available type information with a common dictionary.
   * @param graph input graph
   * @param env environment
   * @return graph with additional internal type property
   */
  public static Graph<Long, ObjectMap, NullValue> applyTypeToInternalTypeMapping(
      Graph<Long, ObjectMap, NullValue> graph,
      ExecutionEnvironment env) {
    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .getVertices()
        .map(new InternalTypeMapFunction());

    return Graph.fromDataSet(vertices, graph.getEdges(), env);
  }

  /**
   * Exclude edges where directly connected source and target vertices have different type property values.
   * @param graph input graph
   * @param isTypeMissMatchCorrectionActive true enables option
   * @return corrected graph
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, NullValue> applyTypeMissMatchCorrection(
      Graph<Long, ObjectMap, NullValue> graph,
      boolean isTypeMissMatchCorrectionActive,
      ExecutionEnvironment env) throws Exception {
    if (isTypeMissMatchCorrectionActive) {
      DataSet<Tuple2<Long, String>> vertexIdAndTypeList = graph.getVertices()
          .flatMap(new VertexIdTypeTupleMapper());

      DataSet<Tuple4<Long, Long, String, String>> edgeTypes = graph.getEdges()
          .map(edge -> new Tuple4<>(edge.getSource(), edge.getTarget(), "", ""))
          .returns(new TypeHint<Tuple4<Long, Long, String, String>>() {})
          .join(vertexIdAndTypeList)
          .where(0).equalTo(0)
          .with(new EdgeTypeJoinFunction(0))
          .distinct()
          .join(vertexIdAndTypeList)
          .where(1)
          .equalTo(0)
          .with(new EdgeTypeJoinFunction(1))
          .distinct();

      DataSet<Edge<Long, NullValue>> edgesEqualType = edgeTypes
          .filter(new EqualTypesEdgeFilterFunction())
          .map(tuple -> new Edge<>(tuple.f0, tuple.f1, NullValue.getInstance()))
          .returns(new TypeHint<Edge<Long, NullValue>>() {})
          .distinct(0, 1);

      DataSet<Vertex<Long, ObjectMap>> resultVertices = deleteVerticesWithoutAnyEdges(
          graph.getVertices(),
          edgesEqualType.<Tuple2<Long, Long>>project(0, 1));

      return Graph.fromDataSet(resultVertices, edgesEqualType, env);

    }

    return graph;
  }

  private static class LinkFilterExcludeEdgeFlatJoinFunction extends RichFlatJoinFunction<Edge<Long,ObjectMap>,
      Tuple1<Long>, Edge<Long, ObjectMap>> {
    private LongCounter filteredLinks = new LongCounter();

    @Override
    public void open(final Configuration parameters) throws Exception {
      super.open(parameters);
      getRuntimeContext().addAccumulator(Utils.PREPROC_LINK_FILTER_ACCUMULATOR, filteredLinks);
    }

    @Override
    public void join(Edge<Long, ObjectMap> left, Tuple1<Long> right,
                     Collector<Edge<Long, ObjectMap>> collector) throws Exception {
      if (right == null) {
        collector.collect(left);
      } else {
        filteredLinks.add(1L);
      }
    }
  }

}
