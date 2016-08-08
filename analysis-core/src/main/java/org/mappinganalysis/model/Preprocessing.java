package org.mappinganalysis.model;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
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
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

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
  public static <EV> Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, EV> graph,
                                                               ExampleOutput out,
                                                               ExecutionEnvironment env) throws Exception {
    if (!Constants.IS_PREPROC_ONLY) {
      return (Graph<Long, ObjectMap, ObjectMap>) graph;
    }
    // todo fix this workaround
    Graph<Long, ObjectMap, NullValue> preGraph = graph
        .mapEdges(new MapFunction<Edge<Long, EV>, NullValue>() { // dont replace
          @Override
          public NullValue map(Edge<Long, EV> value) throws Exception {
            return NullValue.getInstance();
          }
        });

    preGraph = applyTypeToInternalTypeMapping(preGraph, env);

    preGraph = GraphUtils.addCcIdsToGraph(preGraph, env);
//    Utils.writeToFile(graph.getVertices(), "1_input_graph_withCc");
    out.addPreClusterSizes("1 cluster sizes input graph", preGraph.getVertices(), Constants.CC_ID);

    if (Constants.IS_RESTRICT_ACTIVE) {
      preGraph = restrictGraph(preGraph, env);
    }

    /*
     * restrict graph to direct links with matching type information
     */
    preGraph = applyTypeMissMatchCorrection(preGraph, true, env);
    Graph<Long, ObjectMap, ObjectMap> simGraph = Graph.fromDataSet(
        preGraph.getVertices(),
        SimilarityComputation.computeGraphEdgeSim(preGraph, Constants.DEFAULT_VALUE),
        env);
    simGraph = GraphUtils.addCcIdsToGraph(simGraph, env);

    /*
     * restrict (direct) links to 1:1 in terms of sources from one entity to another
     */
    simGraph = applyLinkFilterStrategy(simGraph, env, true);

    return GraphUtils.addCcIdsToGraph(simGraph, env);
  }

  /**
   * Restrict graph for testing purpose. First 500 vertices and contained edges.
   */
  private static Graph<Long, ObjectMap, NullValue> restrictGraph(Graph<Long, ObjectMap, NullValue> graph,
                                                                 ExecutionEnvironment env) {
    // restrict to first ??? clusters
    DataSet<Tuple1<Long>> restrictedComponentIds = graph.getVertices()
        .map(vertex -> new Tuple1<>((long) vertex.getValue().get(Constants.CC_ID)))
        .returns(new TypeHint<Tuple1<Long>>() {})
//        .filter(tuple -> {
//          return tuple.f0 == 1868L;
////            return tuple.f0 == 1134L || tuple.f0 == 60L;// || tuple.f0 == 1135L || tuple.f0 == 8214L; // typegroupby diff
////            return tuple.f0 == 890L || tuple.f0 == 1134L || tuple.f0 == 60L || tuple.f0 == 339L; // typegroupby diff
//        });
        .first(500);

    DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
        .map(vertex -> new Tuple2<>(vertex.getId(), (long) vertex.getValue().get(Constants.CC_ID))) //vid, ccid
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
   * CSV Reader
   * @return graph with vertices and edges.
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, NullValue> getInputGraphFromCsv(
      ExecutionEnvironment env,
      ExampleOutput out)
      throws Exception {
    DataLoader loader = new DataLoader(env);
    final String vertexFile = "concept.csv";
    final String edgeFile = "linksWithIDs.csv";
    final String propertyFile = "concept_attributes.csv";
    final String ccFile = "cc.csv";

    DataSet<Vertex<Long, ObjectMap>> vertices = loader
        .getVerticesFromCsv(Constants.INPUT_DIR + vertexFile, Constants.INPUT_DIR + propertyFile);

    if (Constants.INPUT_DIR.contains("linklion")) {
      if (Constants.LL_MODE.equals("nyt")) {
        vertices = getNytVerticesLinklion(env, ccFile, vertices, out);
      } else if (Constants.LL_MODE.equals("random")) {
        vertices = getRandomCcsFromLinklion(env, ccFile, vertices, 50000);
      }
    }

    DataSet<Edge<Long, NullValue>> edges = deleteEdgesWithoutSourceOrTarget(
        loader.getEdgesFromCsv(Constants.INPUT_DIR + edgeFile), vertices);

    vertices = deleteVerticesWithoutAnyEdges(
        vertices, edges.<Tuple2<Long, Long>>project(0, 1));

    return Graph.fromDataSet(vertices, edges, env);
  }

  /**
   * Restrict LinkLion dataset by taking random CCs
   */
  private static DataSet<Vertex<Long, ObjectMap>> getRandomCcsFromLinklion(
      ExecutionEnvironment env,
      String ccFile,
      DataSet<Vertex<Long, ObjectMap>> vertices,
      int componentNumber) {
    DataSet<Tuple2<Long, Long>> vertexIdAndCcs = getBaseVertexCcs(env, ccFile);

    // todo why not 10k components?

    DataSet<Tuple1<Long>> relevantCcs = vertexIdAndCcs.<Tuple1<Long>>project(1).first(componentNumber);
    vertices = restrictVerticesToGivenCcs(vertices, vertexIdAndCcs, relevantCcs);

    return vertices;
  }

  private static DataSet<Vertex<Long, ObjectMap>> restrictVerticesToGivenCcs(
      DataSet<Vertex<Long, ObjectMap>> vertices,
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
//          LOG.info("restrictedVertex: " + vertex);
          return vertex;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .distinct(0);

    return vertices;
  }

  private static DataSet<Tuple2<Long, Long>> getBaseVertexCcs(ExecutionEnvironment env, String ccFile) {
    return env.readCsvFile(Constants.INPUT_DIR + ccFile)
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
                                                                         DataSet<Vertex<Long, ObjectMap>> vertices, ExampleOutput out) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> nytVertices = vertices.filter(vertex ->
        vertex.getValue().getOntology().equals(Constants.NYT_NS));
    out.addDataSetCount("nyt vertices", nytVertices);

    DataSet<Tuple2<Long, Long>> vertexCcs = getBaseVertexCcs(env, ccFile);

    DataSet<Tuple1<Long>> relevantCcs = vertexCcs.rightOuterJoin(nytVertices)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Vertex<Long, ObjectMap>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<Long, Long> first, Vertex<Long, ObjectMap> second, Collector<Tuple1<Long>> out) throws Exception {
            out.collect(new Tuple1<>(first.f1));
          }
        });


    DataSet<ComponentSourceTuple> componentSourceTuples = getComponentSourceTuples(vertices, vertexCcs);

    /**
     * min size 5
     * first 500
     */
    DataSet<Tuple1<Long>> additionalCcs = vertexCcs.leftOuterJoin(relevantCcs)
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple1<Long>, Tuple2<Long, Long>>() {
          @Override
          public void join(Tuple2<Long, Long> left, Tuple1<Long> right, Collector<Tuple2<Long, Long>> out) throws Exception {
            if (right == null) {
              out.collect(left);
            }
          }
        })
        .<Tuple1<Long>>project(1);

    additionalCcs = additionalCcs
        .leftOuterJoin(componentSourceTuples)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple1<Long>, ComponentSourceTuple, Tuple1<Long>>() {
          @Override
          public void join(Tuple1<Long> left, ComponentSourceTuple right,
                           Collector<Tuple1<Long>> out) throws Exception {
            if (right != null && right.getSourceCount() >= 3) {
              out.collect(left);
            }
          }
        })
        .first(500);

    additionalCcs.print();

//        .groupBy(1)
//        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple1<Long>>() {
//          @Override
//          public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple1<Long>> out) throws Exception {
//            int size = 0;
//            Long comp = null;
//            for (Tuple2<Long, Long> value : values) {
//              ++size;
//              if (comp == null) {
//                comp = value.f1;
//              }
//            }
//            if (size > 4) {
//              out.collect(new Tuple1<>(comp));
//            }
//          }
//        })
//        .first(500)
//        .project(0);

    vertices = restrictVerticesToGivenCcs(vertices, vertexCcs, relevantCcs.union(additionalCcs));

//      out.addDataSetCount("relevant vertices", vertices);
    return vertices;
  }

  public static DataSet<ComponentSourceTuple> getComponentSourceTuples(
      DataSet<Vertex<Long, ObjectMap>> vertices,
      DataSet<Tuple2<Long, Long>> ccs) {

    vertices = vertices.leftOuterJoin(ccs)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Vertex<Long,ObjectMap>, Tuple2<Long,Long>, Vertex<Long, ObjectMap>>() {
          @Override
          public void join(Vertex<Long, ObjectMap> left, Tuple2<Long, Long> right, Collector<Vertex<Long, ObjectMap>> out) throws Exception {
            left.getValue().addProperty(Constants.CC_ID, right.f1);
            out.collect(left);
          }
        });

    return vertices.groupBy(new CcIdKeySelector())
        .reduceGroup(new GroupReduceFunction<Vertex<Long,ObjectMap>, ComponentSourceTuple>() {
          @Override
          public void reduce(Iterable<Vertex<Long, ObjectMap>> vertices, Collector<ComponentSourceTuple> out) throws Exception {
            ComponentSourceTuple result = new ComponentSourceTuple();
            boolean isFirst = true;
            for (Vertex<Long, ObjectMap> vertex : vertices) {
              if (isFirst) {
                result.setCcId(vertex.getValue().getCcId());
                isFirst = false;
              }
              result.addSource(vertex.getValue().getOntology());
            }
            out.collect(result);
          }
        });
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
   * @return output graph
   */
  public static Graph<Long, ObjectMap, ObjectMap> applyLinkFilterStrategy(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env,
      Boolean isDeleteSingleVertices) {
    return firstLinkFilter(graph, env, isDeleteSingleVertices);
  }

  private static Graph<Long, ObjectMap, ObjectMap> secondLinkFilter(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env,
      Boolean isDeleteSingleVertices) {
    // Tuple6(edge src, edge trg, VertexId, Ontology, 1, EdgeSim)
    final DataSet<Tuple6<Long, Long, Long, String, Integer, Double>> basicOneToManyTuples = graph
        .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.ALL);

    DataSet<Tuple3<Long, String, Double>> maxRandomTuples = basicOneToManyTuples
        .groupBy(2, 3)
        .sum(4).andMax(5)
        .filter(tuple -> tuple.f4 > 1)
        .map(tuple -> {
          LOG.info("maxRandomTuple: " + tuple.f2 + ", " + tuple.f3 + ", " + tuple.f5);
          return new Tuple3<>(tuple.f2, tuple.f3, tuple.f5);
        })
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
        .with((first, second) -> {
          LOG.info("###maxRandomTuple### " + second.toString());
          return second;
        })
        .returns(new TypeHint<Edge<Long, ObjectMap>>() {})
        .distinct(0,1);

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

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices();
    if (isDeleteSingleVertices) {
      resultVertices = deleteVerticesWithoutAnyEdges(
          graph.getVertices(),
          newEdges.<Tuple2<Long, Long>>project(0, 1));
    }

    return Graph.fromDataSet(resultVertices, newEdges, env);
  }

  /**
   * old version of link filter method
   * @param graph
   * @param env
   * @param isDeleteSingleVertices
   * @return
   */
  @Deprecated
  private static Graph<Long, ObjectMap, ObjectMap> firstLinkFilter(Graph<Long, ObjectMap, ObjectMap> graph, ExecutionEnvironment env, Boolean isDeleteSingleVertices) {
    // Tuple6(edge src, edge trg, VertexId, Ontology, 1, EdgeSim)
    final DataSet<Tuple6<Long, Long, Long, String, Integer, Double>> basicOneToManyTuples = graph
        .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.ALL);

    DataSet<Tuple3<Long, String, Double>> maxRandomTuples = basicOneToManyTuples
        .groupBy(2, 3)
        .sum(4).andMax(5)
        .filter(tuple -> tuple.f4 > 1)
        .map(tuple -> {
          LOG.info("maxRandomTuple: " + tuple.f2 + ", " + tuple.f3 + ", " + tuple.f5);
          return new Tuple3<>(tuple.f2, tuple.f3, tuple.f5);
        })
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
        .with((first, second) -> {
          LOG.info("###maxRandomTuple### " + second.toString());
          return second;
        })
        .returns(new TypeHint<Edge<Long, ObjectMap>>() {})
        .distinct(0,1);

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

    DataSet<Vertex<Long, ObjectMap>> resultVertices = graph.getVertices();
    if (isDeleteSingleVertices) {
      resultVertices = deleteVerticesWithoutAnyEdges(
          graph.getVertices(),
          newEdges.<Tuple2<Long, Long>>project(0, 1));
    }

    return Graph.fromDataSet(resultVertices, newEdges, env);
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
      getRuntimeContext().addAccumulator(Constants.PREPROC_LINK_FILTER_ACCUMULATOR, filteredLinks);
    }

    @Override
    public void join(Edge<Long, ObjectMap> left, Tuple1<Long> right,
                     Collector<Edge<Long, ObjectMap>> collector) throws Exception {
      if (right == null) {
        LOG.info("LFExclude: " + left.toString());
        collector.collect(left);
      } else {
        filteredLinks.add(1L);
      }
    }
  }

}
