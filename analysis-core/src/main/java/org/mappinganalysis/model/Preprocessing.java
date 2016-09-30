package org.mappinganalysis.model;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
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
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.functions.preprocessing.*;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.SourcesUtils;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Preprocessing.
 */
public class Preprocessing {
  private static final Logger LOG = Logger.getLogger(Preprocessing.class);

  /**
   * Execute all preprocessing steps with the given options
   */
  public static <EV> Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, EV> inGraph,
                                                               String verbosity,
                                                               ExampleOutput out,
                                                               ExecutionEnvironment env) throws Exception {
    Graph<Long, ObjectMap, NullValue> graph = GraphUtils.mapEdgesToNullValue(inGraph);
    graph = removeEqualSourceLinks(
        graph.getEdgeIds(),
        applyTypeToInternalTypeMapping(graph),
        env);

    // stats start
    // TODO cc computation produces memory an out exception, dont use
    if (verbosity.equals(Constants.DEBUG)) {
      graph = GraphUtils.addCcIdsToGraph(graph, env); // only needed for stats
      out.addPreClusterSizes("1 cluster sizes input graph", graph.getVertices(), Constants.CC_ID);
    }
    // stats end

    /*
     * restrict graph to direct links with matching type information
     */
    graph = applyTypeMissMatchCorrection(graph, true, env);
    Graph<Long, ObjectMap, ObjectMap> simGraph = Graph.fromDataSet(
        graph.getVertices(),
        SimilarityComputation.computeGraphEdgeSim(graph, Constants.DEFAULT_VALUE),
        env);

    /*
     * restrict (direct) links to 1:1 in terms of sources from one entity to another
     */

    return applyLinkFilterStrategy(simGraph, env, true);
  }

  /**
   * Remove links where source and target dataset name are equal, remove duplicate links
   */
  public static Graph<Long, ObjectMap, NullValue> removeEqualSourceLinks(
      DataSet<Tuple2<Long, Long>> edgeIds,
      DataSet<Vertex<Long, ObjectMap>> vertices,
      ExecutionEnvironment env) {
    DataSet<Edge<Long, NullValue>> edges = getEdgeIdSourceValues(edgeIds, vertices)
        .filter(edge -> !edge.getSrcSource().equals(edge.getTrgSource()))
        .map(value -> new Edge<>(value.f0, value.f1, NullValue.getInstance()))
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .distinct();

    return Graph.fromDataSet(vertices, edges, env);
  }

  /**
   * Create a dataset of edge ids with the associated dataset source values like "http://dbpedia.org/
   */
  public static DataSet<EdgeIdsSourcesTuple> getEdgeIdSourceValues(
      DataSet<Tuple2<Long, Long>> edgeIds,
      DataSet<Vertex<Long, ObjectMap>> vertices) {
    return edgeIds
        .map(edge -> new EdgeIdsSourcesTuple(edge.f0, edge.f1, "", ""))
        .returns(new TypeHint<EdgeIdsSourcesTuple>() {})
        .join(vertices)
        .where(0)
        .equalTo(0)
        .with((tuple, vertex) -> {
          tuple.checkSideAndUpdate(0, vertex.getValue().getOntology());
          return tuple;
        })
        .returns(new TypeHint<EdgeIdsSourcesTuple>() {})
        .join(vertices)
        .where(1)
        .equalTo(0)
        .with((tuple, vertex) -> {
          tuple.checkSideAndUpdate(1, vertex.getValue().getOntology());
          return tuple;
        })
        .returns(new TypeHint<EdgeIdsSourcesTuple>() {});
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
      if (Constants.LL_MODE.equals("nyt")
          || Constants.LL_MODE.equals("write")
          || Constants.LL_MODE.equals("print")
          || Constants.LL_MODE.equals("plan")) {
        vertices = getNytVerticesLinklion(env, ccFile, vertices, out); // only 500!!
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
        .with((id, vertex) -> vertex)
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
   *
   * TODO test lambdas
   */
  private static DataSet<Vertex<Long, ObjectMap>> getNytVerticesLinklion(
      ExecutionEnvironment env,
      String ccFile,
      DataSet<Vertex<Long, ObjectMap>> vertices,
      ExampleOutput out) throws Exception {
    DataSet<Vertex<Long, ObjectMap>> nytVertices = vertices.filter(vertex ->
        vertex.getValue().getOntology().equals(Constants.NYT_NS));
//    out.addDataSetCount("nyt vertices", nytVertices);

    DataSet<Tuple2<Long, Long>> vertexCcs = getBaseVertexCcs(env, ccFile);

    DataSet<Tuple1<Long>> relevantCcs = vertexCcs.rightOuterJoin(nytVertices)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Vertex<Long, ObjectMap>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<Long, Long> first, Vertex<Long, ObjectMap> second,
                           Collector<Tuple1<Long>> out) throws Exception {
            out.collect(new Tuple1<>(first.f1));
          }
        });


    DataSet<ComponentSourceTuple> componentSourceTuples = getComponentSourceTuples(vertices, vertexCcs);

    DataSet<Tuple1<Long>> additionalCcs = vertexCcs.leftOuterJoin(relevantCcs)
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple1<Long>, Tuple2<Long, Long>>() {
          @Override
          public void join(Tuple2<Long, Long> left,
                           Tuple1<Long> right,
                           Collector<Tuple2<Long, Long>> out) throws Exception {
            if (right == null) {
              out.collect(left);
            }
          }
        })
        .<Tuple1<Long>>project(1)
        .distinct();

    /**
     * now based on sources count in component, we want at least 3 different sources
     */
    additionalCcs = additionalCcs
        .join(componentSourceTuples)
        .where(0)
        .equalTo(0)
        .with((Tuple1<Long> tuple, ComponentSourceTuple compTuple, Collector<Tuple1<Long>> collector) -> {
          if (SourcesUtils.getSourceCount(compTuple) >= 3) {
            collector.collect(tuple);
          }
        })
        .returns(new TypeHint<Tuple1<Long>>() {})
//        .with(new FlatJoinFunction<Tuple1<Long>, ComponentSourceTuple, Tuple1<Long>>() {
//          @Override
//          public void join(Tuple1<Long> left,
//                           ComponentSourceTuple right,
//                           Collector<Tuple1<Long>> out) throws Exception {
//            if (right != null && SourcesUtils.getSourceCount(right) >= 3) {
//              out.collect(left);
//            }
//          }
//        })
        .first(500);

    return restrictVerticesToGivenCcs(vertices, vertexCcs, relevantCcs.union(additionalCcs));
  }

  public static DataSet<ComponentSourceTuple> getComponentSourceTuples(
      DataSet<Vertex<Long, ObjectMap>> vertices,
      DataSet<Tuple2<Long, Long>> ccs) {

    if (ccs != null) {
      vertices = vertices.leftOuterJoin(ccs)
          .where(0)
          .equalTo(0)
          .with(new FlatJoinFunction<Vertex<Long, ObjectMap>,
              Tuple2<Long, Long>,
              Vertex<Long, ObjectMap>>() {
            @Override
            public void join(Vertex<Long, ObjectMap> left, Tuple2<Long, Long> right, Collector<Vertex<Long, ObjectMap>> out) throws Exception {
              left.getValue().addProperty(Constants.CC_ID, right.f1);
              out.collect(left);
            }
          });
    }

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
   * TODO fix lambdas not working because of type erasure, even with TypeHint
   */
  public static <EV> DataSet<Edge<Long, EV>> deleteEdgesWithoutSourceOrTarget(
      DataSet<Edge<Long, EV>> edges, DataSet<Vertex<Long, ObjectMap>> vertices) {

    return edges.join(vertices)
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<Edge<Long,EV>, Vertex<Long,ObjectMap>, Edge<Long, EV>>() {
          @Override
          public Edge<Long, EV> join(Edge<Long, EV> first, Vertex<Long, ObjectMap> second) throws Exception {
            return first;
          }
        })
        .join(vertices)
        .where(1)
        .equalTo(0)
        .with(new JoinFunction<Edge<Long, EV>, Vertex<Long, ObjectMap>, Edge<Long, EV>>() {
          @Override
          public Edge<Long, EV> join(Edge<Long, EV> first, Vertex<Long, ObjectMap> second) throws Exception {
            return first;
          }
        })
        .distinct(0,1);
    // not working
//        .with((edge, vertex) -> edge)
//        .returns(new TypeHint<Edge<Long, EV>>() {
//        });
    // old
//        .with(new EdgeRestrictFlatJoinFunction())
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
        .join(edges)
        .where(0)
        .equalTo(0)
        .with((vertex, edge) -> vertex)
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    return vertices
        .join(edges)
        .where(0)
        .equalTo(1)
        .with((vertex, edge) -> vertex)
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .union(left)
        .distinct(0);
  }

  /**
   * Create the input graph for further analysis,
   * restrict to edges where source and target are in vertices set.
   * @param fullDbString complete server+port+db string
   * @deprecated load from db too slow
   */
  @Deprecated
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
      Boolean isDeleteSingleVertices) throws Exception {
    return secondLinkFilter(graph, env, isDeleteSingleVertices);
  }

  /**
   * currently in use simple 1:n removal
   * TODO grouping based on ccId still used for creating independent blocks, how to avoid?
   */
  private static Graph<Long, ObjectMap, ObjectMap> secondLinkFilter(
      Graph<Long, ObjectMap, ObjectMap> graph,
      ExecutionEnvironment env,
      Boolean isDeleteSingleVertices) throws Exception {

    graph = GraphUtils.addCcIdsToGraph(graph, env);

    // EdgeSourceSimTuple(edge src, edge trg, vertex ont, neighbor ont, EdgeSim)
    DataSet<EdgeSourceSimTuple> neighborTuples = graph
        .groupReduceOnNeighbors(new SecondNeighborOntologyFunction(), EdgeDirection.OUT);

    DataSet<Tuple2<Long, Long>> edgeTuples = neighborTuples.groupBy(0)
        .sortGroup(5, Order.DESCENDING)
        .sortGroup(1, Order.ASCENDING)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup(new LinkSelectionWithCcIdFunction());

    DataSet<Edge<Long, ObjectMap>> newEdges = edgeTuples.join(graph.getEdges())
        .where(0, 1)
        .equalTo(0, 1)
        .with((tuple, edge) -> edge)
        .returns(new TypeHint<Edge<Long, ObjectMap>>() {});

    DataSet<Vertex<Long, ObjectMap>> resultVertices;
    if (isDeleteSingleVertices) {
      resultVertices = deleteVerticesWithoutAnyEdges(
          graph.getVertices(),
          newEdges.<Tuple2<Long, Long>>project(0, 1));
    } else {
      resultVertices = graph.getVertices();
    }

    return Graph.fromDataSet(resultVertices, newEdges, env);
  }

//  /**
//   * template wip copy of secondfilter approach with sets for each link way
//   * @param graph
//   * @param env
//   * @param isDeleteSingleVertices
//   * @return
//   */
//  private static Graph<Long, ObjectMap, ObjectMap> thirdLinkFilter(
//      Graph<Long, ObjectMap, ObjectMap> graph,
//      ExecutionEnvironment env,
//      Boolean isDeleteSingleVertices) {
//    // Tuple6(ccId, edge src, edge trg, vertex ont, neighbor ont, EdgeSim)
//    final DataSet<EdgeSourceSimTuple> neighborTuples = graph
//        .groupReduceOnNeighbors(new SecondNeighborOntologyFunction(), EdgeDirection.OUT);
//
//
//    DataSet<Tuple3<Long, Long, Long>> edgeTuples = neighborTuples.groupBy(0)
//        .sortGroup(5, Order.DESCENDING)
////        .sortGroup(1, Order.DESCENDING)
////        .sortGroup(2, Order.DESCENDING)
//        .reduceGroup(new GroupReduceFunction<EdgeSourceSimTuple,
//            Tuple3<Long, Long, Long>>() {
//          @Override
//          public void reduce(Iterable<EdgeSourceSimTuple> values,
//                             Collector<Tuple3<Long, Long, Long>> out) throws Exception {
//
//            HashMap<Long, ComponentSourceTuple> sourcesMap = Maps.newHashMap();
//            HashMap<Long, String> ownType = Maps.newHashMap();
//            // ccid, e.src, e.trg, v.src, e.src, sim
//
//            for (EdgeSourceSimTuple link : values) {
//              Tuple3<Long, Long, Long> tmpResult
//                  = new Tuple3<>(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE);
//              ComponentSourceTuple srcCst = sourcesMap.get(link.getSrcId());
//              ComponentSourceTuple trgCst = sourcesMap.get(link.getTrgId());
//
//              LOG.info("###nnof: " + link.toString());
//              if (!sourcesMap.containsKey(link.getSrcId())) {
//                ComponentSourceTuple src = new ComponentSourceTuple(link.getSrcId());
//                ownType.put(link.getSrcId(), link.getSrcOntology());
//                src.addSource(link.getSrcOntology());
//                src.addSource(link.getTrgOntology());
//                sourcesMap.put(link.getSrcId(), src);
//
//                tmpResult.f1 = link.getSrcId();
//              } else {
//                if (srcCst.contains(link.getTrgOntology())) {
//                  // do not create link
//                }
//              }
//              if (!sourcesMap.containsKey(link.getTrgId())) {
//                ComponentSourceTuple trg = new ComponentSourceTuple(link.getTrgId());
//                ownType.put(link.getTrgId(), link.getTrgOntology());
//                trg.addSource(link.getSrcOntology());
//                trg.addSource(link.getTrgOntology());
//                sourcesMap.put(link.getTrgId(), trg);
//
//                tmpResult.f2 = link.getTrgId();
//              } else {
//                if (trgCst.contains(link.getSrcOntology())) {
//                  // do not create link
//                }
//              }
//
//              Set<String> srcSources = srcCst.getSources();
//              for (String srcSource : srcSources) {
//                if (trgCst.contains(srcSource)) {
//                  // if true, do not create
//                }
//              }
//
//            }
//          }
//        });
//
//    DataSet<Edge<Long, ObjectMap>> newEdges = edgeTuples.leftOuterJoin(graph.getEdges())
//        .where(0, 1)
//        .equalTo(0, 1)
//        .with((left, right) -> right)
//        .returns(new TypeHint<Edge<Long, ObjectMap>>() {
//        });
//
//    DataSet < Vertex < Long, ObjectMap >> resultVertices = graph.getVertices();
//    if (isDeleteSingleVertices) {
//      resultVertices = deleteVerticesWithoutAnyEdges(
//          graph.getVertices(),
//          newEdges.<Tuple2<Long, Long>>project(0, 1));
//    }
//
//    return Graph.fromDataSet(resultVertices, newEdges, env);
//  }

  /**
   * Harmonize available type information with a common dictionary.
   * @param graph input graph
   * @return graph with additional internal type property
   */
  public static DataSet<Vertex<Long, ObjectMap>> applyTypeToInternalTypeMapping(
      Graph<Long, ObjectMap, NullValue> graph) {
    return graph.getVertices()
        .map(new InternalTypeMapFunction());
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
      DataSet<IdTypeTuple> vertexIdAndTypeList = graph.getVertices()
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


}
