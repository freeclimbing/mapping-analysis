package org.mappinganalysis.model;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.FlinkConnectedComponents;
import org.mappinganalysis.io.DataLoader;
import org.mappinganalysis.io.functions.EdgeRestrictFlatJoinFunction;
import org.mappinganalysis.io.functions.VertexRestrictFlatJoinFunction;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.functions.CcIdVertexJoinFunction;
import org.mappinganalysis.model.functions.VertexIdMapFunction;
import org.mappinganalysis.model.functions.preprocessing.*;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.utils.Utils;
import org.mappinganalysis.utils.functions.keyselector.CcIdAndCompTypeKeySelector;

/**
 * Preprocessing.
 */
public class Preprocessing {
  private static final Logger LOG = Logger.getLogger(Preprocessing.class);

  /**
   * Execute all preprocessing steps with the given options
   * @param graph input graph
   * @param isLinkFilterActive should links with duplicate entries per dataset be deleted
   * @param env execution environment  @return graph
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, ObjectMap> execute(Graph<Long, ObjectMap, NullValue> graph,
                                                          boolean isLinkFilterActive,
                                                          ExecutionEnvironment env, ExampleOutput out) throws Exception {
    graph = applyTypeToInternalTypeMapping(graph, env);
    graph = addCcIdsToGraph(graph);
    Utils.writeToHdfs(graph.getVertices(), "1_input_graph_withCc");
    out.addPreClusterSizes("1 cluster sizes input graph", graph.getVertices(), Utils.CC_ID);

//    graph = restrictGraph(graph, out, env);

    graph = applyTypeMissMatchCorrection(graph, true, env);
    Graph<Long, ObjectMap, ObjectMap> simGraph = SimilarityComputation.initSimilarity(graph, env);

    simGraph = applyLinkFilterStrategy(simGraph, env, isLinkFilterActive);
    simGraph = addCcIdsToGraph(simGraph);

    DataSet<Vertex<Long, ObjectMap>> vertices = simGraph.getVertices()
        .map(new AddShadingTypeMapFunction())
        .groupBy(new CcIdAndCompTypeKeySelector())
        .reduceGroup(new GenerateHashCcIdGroupReduceFunction());

    return Graph.fromDataSet(vertices, simGraph.getEdges(), env);
  }

  private static Graph<Long, ObjectMap, NullValue> restrictGraph(Graph<Long, ObjectMap, NullValue> graph,
                                                                 ExampleOutput out, ExecutionEnvironment env) {
    // restrict to first 100k clusters
    DataSet<Tuple1<Long>> restrictedComponentIds = graph.getVertices()
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple1<Long>>() {
          @Override
          public Tuple1<Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Tuple1<>((long) vertex.getValue().get(Utils.CC_ID));
          }
        })
        .filter(new FilterFunction<Tuple1<Long>>() {
          @Override
          public boolean filter(Tuple1<Long> longTuple1) throws Exception {
            return longTuple1.f0 == 122L;
          }
        });
//        .first(100000);

    DataSet<Vertex<Long, ObjectMap>> newVertices = graph.getVertices()
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Tuple2<>(vertex.getId(), (long) vertex.getValue().get(Utils.CC_ID));
          }
        }) //vid, ccid
        .join(restrictedComponentIds)
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Tuple2<Long, Long>, Tuple1<Long>, Tuple1<Long>>() {
          @Override
          public void join(Tuple2<Long, Long> left, Tuple1<Long> right, Collector<Tuple1<Long>> collector) throws Exception {
            collector.collect(new Tuple1<>(left.f0));
          }
        })
        .leftOuterJoin(graph.getVertices())
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<Tuple1<Long>, Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> join(Tuple1<Long> longTuple1, Vertex<Long, ObjectMap> vertex) throws Exception {
            return vertex;
          }
        });

//    Utils.writeToHdfs(newVertices, "newVertices");

    DataSet<Edge<Long, NullValue>> newEdges = deleteEdgesWithoutSourceOrTarget(graph, newVertices);

    graph = Graph.fromDataSet(newVertices, newEdges, env);
    out.addVertexAndEdgeSizes("afterInitialVertexDeletionAndRestrictedClusters", graph);
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

    DataSet<Vertex<Long, ObjectMap>> vertices = loader
        .getVerticesFromCsv(Utils.INPUT_DIR + vertexFile, Utils.INPUT_DIR + propertyFile);

//    Utils.writeToHdfs(vertices, "inputVertices");

    // restrict edges to these where source and target are vertices
    DataSet<Edge<Long, NullValue>> edges = loader.getEdgesFromCsv(Utils.INPUT_DIR + edgeFile)
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

  // not yet working correctly
  public static DataSet<Edge<Long, NullValue>> deleteEdgesWithoutSourceOrTarget(Graph<Long, ObjectMap, NullValue> graph, DataSet<Vertex<Long, ObjectMap>> newVertices) {
    return graph.getEdges()
        .leftOuterJoin(newVertices)
        .where(0).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction())
        .leftOuterJoin(newVertices)
        .where(1).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction());
  }

  /**
   * delete vertices without any edges
   * @param vertices input vertices
   * @param edges edge set
   * @return vertices
   */
  public static DataSet<Vertex<Long, ObjectMap>> deleteVerticesWithoutAnyEdges(
      DataSet<Vertex<Long, ObjectMap>> vertices, DataSet<Tuple2<Long, Long>> edges) {

    DataSet<Vertex<Long, ObjectMap>> left = vertices
        .leftOuterJoin(edges)
        .where(0).equalTo(0)
        .with(new VertexRestrictFlatJoinFunction()).distinct(0);

    return vertices
        .leftOuterJoin(edges)
        .where(0).equalTo(1)
        .with(new VertexRestrictFlatJoinFunction()).distinct(0)
        .union(left);
  }

  /**
   * Create the input graph for further analysis,
   * restrict to edges where source and target are in vertices set.
   * @return graph with vertices and edges.
   * @throws Exception
   * @param fullDbString complete server+port+db string
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
   * Add initial component ids to vertices based on flink connected components.
   * @param graph input graph
   * @return graph containing vertices with additional property
   * @throws Exception
   */
  public static <T> Graph<Long, ObjectMap, T> addCcIdsToGraph(
      Graph<Long, ObjectMap, T> graph) throws Exception {

    final DataSet<Tuple2<Long, Long>> components = FlinkConnectedComponents
        .compute(graph.getVertices().map(new VertexIdMapFunction()),
            graph.getEdgeIds(),
            1000);

    return graph.joinWithVertices(components, new CcIdVertexJoinFunction());
  }

  /**
   * Preprocessing strategy to restrict resources to have only one counterpart in every target ontology.
   *
   * First strategy: delete all links which are involved in 1:n mappings
   * @param graph input graph
   * @param env environment
   * @param isLinkFilterActive boolean if filter should be used
   * @return output graph
   */
  public static Graph<Long, ObjectMap, ObjectMap> applyLinkFilterStrategy(
      Graph<Long, ObjectMap, ObjectMap> graph, ExecutionEnvironment env,
      boolean isLinkFilterActive) throws Exception {

    if (isLinkFilterActive) {
      DataSet<Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long>> oneToManyTuples = graph
          .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.ALL)
          .groupBy(1, 2)
          .aggregate(Aggregations.SUM, 3)//.andMax(4);
          .filter(new FilterFunction<Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long>>() {
            @Override
            public boolean filter(Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long> tuple) throws Exception {
              return tuple.f3 > 1;
            }
          });

      DataSet<Edge<Long, ObjectMap>> newEdges = graph.getEdges()
          .leftOuterJoin(oneToManyTuples.<Tuple1<Long>>project(1))
          .where(0)
          .equalTo(0)
          .with(new LinkFilterExcludeEdgeFlatJoinFunction())
          .leftOuterJoin(oneToManyTuples.<Tuple1<Long>>project(1))
          .where(1)
          .equalTo(0)
          .with(new LinkFilterExcludeEdgeFlatJoinFunction());

      DataSet<VertexComponentTuple2> oneToManyVertexComponentIds = oneToManyTuples
          .map(new MapFunction<Tuple6<Edge<Long,ObjectMap>,Long,String,Integer,Double,Long>, VertexComponentTuple2>() {
        @Override
        public VertexComponentTuple2 map(Tuple6<Edge<Long, ObjectMap>, Long, String, Integer, Double, Long> tuple) throws Exception {
          return new VertexComponentTuple2(tuple.f1, tuple.f5);
        }
      });

//      Utils.writeRemovedEdgesToHDFS(graph, oneToManyVertexComponentIds, Utils.CC_ID, out);

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
      Graph<Long, ObjectMap, NullValue> graph, ExecutionEnvironment env) {
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
  public static Graph<Long, ObjectMap, NullValue> applyTypeMissMatchCorrection(Graph<Long, ObjectMap, NullValue> graph,
      boolean isTypeMissMatchCorrectionActive, ExecutionEnvironment env) throws Exception {
    if (isTypeMissMatchCorrectionActive) {
      DataSet<Tuple2<Long, String>> vertexIdAndTypeList = graph.getVertices()
          .map(new VertexIdTypeTupleMapper());

      DataSet<Tuple4<Long, Long, String, String>> edgeTypes = graph.getEdges()
          .map(new MapFunction<Edge<Long, NullValue>, Tuple4<Long, Long, String, String>>() {
            @Override
            public Tuple4<Long, Long, String, String> map(Edge<Long, NullValue> edge) throws Exception {
              return new Tuple4<>(edge.getSource(), edge.getTarget(), "", "");
            }
          })
          .join(vertexIdAndTypeList)
          .where(0).equalTo(0)
          .with(new EdgeTypeJoinFunction(0))
          .distinct(0, 1)
          .join(vertexIdAndTypeList)
          .where(1)
          .equalTo(0)
          .with(new EdgeTypeJoinFunction(1))
          .distinct(0, 1);

      DataSet<Edge<Long, NullValue>> edgesEqualType = edgeTypes
          .filter(new FilterEqualTypeEdges())
          .map(new MapFunction<Tuple4<Long, Long, String, String>, Edge<Long, NullValue>>() {
            @Override
            public Edge<Long, NullValue> map(Tuple4<Long, Long, String, String> tuple) throws Exception {
              return new Edge<>(tuple.f0, tuple.f1, NullValue.getInstance());
            }
          })
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
      getRuntimeContext().addAccumulator(Utils.LINK_FILTER_ACCUMULATOR, filteredLinks);
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
