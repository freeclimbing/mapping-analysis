package org.mappinganalysis.model;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.FlinkConnectedComponents;
import org.mappinganalysis.io.JDBCDataLoader;
import org.mappinganalysis.io.functions.EdgeRestrictFlatJoinFunction;
import org.mappinganalysis.io.functions.VertexRestrictFlatJoinFunction;
import org.mappinganalysis.model.functions.CcIdVertexJoinFunction;
import org.mappinganalysis.model.functions.VertexIdMapFunction;
import org.mappinganalysis.model.functions.preprocessing.*;

/**
 * Preprocessing.
 */
public class Preprocessing {
  private static final Logger LOG = Logger.getLogger(Preprocessing.class);

  /**
   * Execute all preprocessing steps with the given options
   * @param graph input graph
   * @param isLinkFilterActive should links with duplicate entries per dataset be deleted
   * @param env execution environment
   * @return graph
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, NullValue> execute(Graph<Long, ObjectMap, NullValue> graph,
                                                          boolean isLinkFilterActive,
                                                          ExecutionEnvironment env) throws Exception {
    graph = applyTypeToInternalTypeMapping(graph, env);
//    graph = applyLinkFilterStrategy(graph, env, isLinkFilterActive);
    graph = applyTypeMissMatchCorrection(graph, true, env);

    return addCcIdsToGraph(graph);
  }

  /**
   * CSV Reader todo fix duplicate code
   * @return graph with vertices and edges.
   * @throws Exception
   * @param inputDir
   */
  public static Graph<Long, ObjectMap, NullValue> getInputGraphFromCsv(String inputDir, ExecutionEnvironment env)
      throws Exception {
    JDBCDataLoader loader = new JDBCDataLoader(env);
    final String vertexFile = "concept.csv";
    final String edgeFile = "linksWithIDs.csv";
    final String propertyFile = "concept_attributes.csv";

    DataSet<Vertex<Long, ObjectMap>> vertices = loader
        .getVerticesFromCsv(inputDir + vertexFile, inputDir + propertyFile);

    // restrict edges to these where source and target are vertices
    DataSet<Edge<Long, NullValue>> edges = loader.getEdgesFromCsv(inputDir + edgeFile)
        .leftOuterJoin(vertices)
        .where(0).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction())
        .leftOuterJoin(vertices)
        .where(1).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction());

    return Graph.fromDataSet(deleteVerticesWithoutAnyEdges(vertices, edges), edges, env);
  }

  /**
   * delete vertices without any edges
   * @param vertices input vertices
   * @param edges edge set
   * @return vertices
   */
  private static DataSet<Vertex<Long, ObjectMap>> deleteVerticesWithoutAnyEdges(
      DataSet<Vertex<Long, ObjectMap>> vertices, DataSet<Edge<Long, NullValue>> edges) {

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
    JDBCDataLoader loader = new JDBCDataLoader(env);
    DataSet<Vertex<Long, ObjectMap>> vertices = loader.getVertices(fullDbString);

    // restrict edges to these where source and target are vertices
    DataSet<Edge<Long, NullValue>> edges = loader.getEdges(fullDbString)
        .leftOuterJoin(vertices)
        .where(0).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction())
        .leftOuterJoin(vertices)
        .where(1).equalTo(0)
        .with(new EdgeRestrictFlatJoinFunction());

    return Graph.fromDataSet(deleteVerticesWithoutAnyEdges(vertices, edges), edges, env);
  }

  /**
   * Add initial component ids to vertices based on flink connected components.
   * @param graph input graph
   * @return graph containing vertices with additional property
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, NullValue> addCcIdsToGraph(
      Graph<Long, ObjectMap, NullValue> graph) throws Exception {

    final DataSet<Tuple2<Long, Long>> components = FlinkConnectedComponents
        .compute(graph.getVertices().map(new VertexIdMapFunction()), graph.getEdgeIds(), 1000);

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
  public static Graph<Long, ObjectMap, NullValue> applyLinkFilterStrategy(
      Graph<Long, ObjectMap, NullValue> graph, ExecutionEnvironment env,
      boolean isLinkFilterActive) {
    if (isLinkFilterActive) {
      LOG.info("[1] Apply basic link filter strategy");
      DataSet<Edge<Long, NullValue>> edgesNoDuplicates = graph
          .groupReduceOnNeighbors(new NeighborOntologyFunction(), EdgeDirection.OUT)
          .groupBy(1, 2)
          .aggregate(Aggregations.SUM, 3)
          .filter(new ExcludeOneToManyOntologiesFilter()) // deleted links accumulator
          .map(new MapFunction<Tuple4<Edge<Long, NullValue>, Long, String, Integer>,
              Edge<Long, NullValue>>() {
            @Override
            public Edge<Long, NullValue> map(Tuple4<Edge<Long, NullValue>, Long, String, Integer> tuple)
                throws Exception {
              return tuple.f0;
            }
          });

      return Graph.fromDataSet(graph.getVertices(), edgesNoDuplicates, env);
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
    LOG.info("[1] Apply type preprocessing");
    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .getVertices()
        .map(new InternalTypeMapFunction());

    return Graph.fromDataSet(vertices, graph.getEdges(), env);
  }

  /**
   * Exclude edges where directly connected source and target vertices have different type property values.
   * @param graph input graph
   * @param isTypeMissMatchCorrectionActive can be disabled in options
   * @return corrected graph
   * @throws Exception
   */
  public static Graph<Long, ObjectMap, NullValue> applyTypeMissMatchCorrection(Graph<Long, ObjectMap, NullValue> graph,
      boolean isTypeMissMatchCorrectionActive, ExecutionEnvironment env) throws Exception {
    if (isTypeMissMatchCorrectionActive) {
      DataSet<Tuple2<Long, String>> vertexIdAndTypeList = graph.getVertices()
          .map(new VertexIdTypeTupleMapper());

      DataSet<Edge<Long, NullValue>> edgesEqualType = graph.getEdges()
          .map(new MapFunction<Edge<Long, NullValue>, Tuple4<Long, Long, String, String>>() {
            @Override
            public Tuple4<Long, Long, String, String> map(Edge<Long, NullValue> edge) throws Exception {
              return new Tuple4<>(edge.getSource(), edge.getTarget(), "", "");
            }
          })
          .join(vertexIdAndTypeList)
          .where(0).equalTo(0)
          .with(new EdgeTypeJoinFunction(0))
          .join(vertexIdAndTypeList).where(1).equalTo(0)
          .with(new EdgeTypeJoinFunction(1))
          .filter(new FilterEqualTypeEdges())
          .map(new MapFunction<Tuple4<Long, Long, String, String>, Edge<Long, NullValue>>() {
            @Override
            public Edge<Long, NullValue> map(Tuple4<Long, Long, String, String> tuple) throws Exception {
              return new Edge<>(tuple.f0, tuple.f1, NullValue.getInstance());
            }
          });

      DataSet<Vertex<Long, ObjectMap>> resultVertices = deleteVerticesWithoutAnyEdges(graph.getVertices(),
          edgesEqualType);

      return Graph.fromDataSet(resultVertices, edgesEqualType, env);

    }


    return graph;
  }
}
