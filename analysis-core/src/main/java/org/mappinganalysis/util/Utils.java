package org.mappinganalysis.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.EdgeComponentTuple3;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.VertexComponentTuple2;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.simplifiers.Simplifiers;
import org.simmetrics.tokenizers.Tokenizers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.simmetrics.builders.StringMetricBuilder.with;

public class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class);

  private static final HashFunction HF = Hashing.md5();

  public static <T> void writeToFile(DataSet<T> data, String outDir) {
    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {
      data.writeAsFormattedText(Constants.INPUT_DIR + "output/" + outDir,
          FileSystem.WriteMode.OVERWRITE,
          new DataSetTextFormatter<>());
    }
  }

  /**
   * Write a Gelly graph to JSON
   */
  public static <EV> void writeGraphToJSONFile(Graph<Long, ObjectMap, EV> graph, String outDir) {
      String vertexOutFile = Constants.INPUT_DIR + "output/" + outDir + "/vertices/";
      String edgeOutFile = Constants.INPUT_DIR + "output/" + outDir + "/edges/";
      JSONDataSink dataSink = new JSONDataSink(vertexOutFile, edgeOutFile);

      dataSink.writeGraph(graph);
  }

  /**
   * Write a DataSet of vertices to JSON
   */
  public static void writeVerticesToJSONFile(DataSet<Vertex<Long, ObjectMap>> vertices, String outDir) {
      String vertexOutFile = Constants.INPUT_DIR + "output/" + outDir + "/";
      JSONDataSink dataSink = new JSONDataSink(vertexOutFile);

      dataSink.writeVertices(vertices);
  }

  public static DataSet<Vertex<Long, ObjectMap>> readVerticesFromJSONFile(
      String verticesPath,
      ExecutionEnvironment env,
      boolean isAbsolutePath) {
    String vertexOutFile = getFinalPath(verticesPath, isAbsolutePath);

    JSONDataSource jsonDataSource = new JSONDataSource(vertexOutFile, env);

    return jsonDataSource.getVertices();
  }

  public static String getFinalPath(String verticesPath, boolean isAbsolutePath) {
    if (!verticesPath.endsWith("/")) {
      verticesPath = verticesPath.concat("/");
    }
    verticesPath = verticesPath.concat("vertices/");
    if (!isAbsolutePath) {
      verticesPath = Constants.INPUT_DIR + "output/" + verticesPath;
    }
    return verticesPath;
  }

  /**
   * Read Gelly graph from JSON file
   * @param inDir path is not absolute
   */
  public static Graph<Long, ObjectMap, ObjectMap> readFromJSONFile(String inDir, ExecutionEnvironment env) {
    return readFromJSONFile(inDir, env, false);
  }

  /**
   * Read Gelly graph from JSON file
   * @param graphPath absolute or relative path
   * @param env execution environment
   * @param isAbsolutePath
   * @return
   */
  public static Graph<Long, ObjectMap, ObjectMap> readFromJSONFile(
      String graphPath,
      ExecutionEnvironment env,
      boolean isAbsolutePath) {

    if (!graphPath.endsWith("/")) {
      graphPath = graphPath.concat("/");
    }
    String vertexOutFile = graphPath.concat("vertices/");
    String edgeOutFile = graphPath.concat("edges/");
    if (!isAbsolutePath) {
      vertexOutFile = Constants.INPUT_DIR + "output/" + vertexOutFile;
      edgeOutFile = Constants.INPUT_DIR + "output/" + edgeOutFile;
    }

    JSONDataSource jsonDataSource = new JSONDataSource(vertexOutFile, edgeOutFile, env);

    return jsonDataSource.getGraph();
  }

  public static class DataSetTextFormatter<V> implements
      TextOutputFormat.TextFormatter<V> {
    @Override
    public String format(V v) {
      return v.toString();
    }
  }

  public static boolean isValidLatitude(Double latitude) {
    return latitude != null && Doubles.compare(latitude, 90) <= 0 && Doubles.compare(latitude, -90) >= 0;
  }

  public static boolean isValidLongitude(Double longitude) {
    return longitude != null && Doubles.compare(longitude, 180) <= 0 && Doubles.compare(longitude, -180) >= 0;
  }

  @Deprecated
  public static void writeRemovedEdgesToHDFS(
      Graph<Long, ObjectMap, ObjectMap> graph,
      DataSet<VertexComponentTuple2> oneToManyVertexComponentIds,
      String componentIdName, ExampleOutput out) {
    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {
      DataSet<VertexComponentTuple2> vertexComponentIds = graph.getVertices()
          .map(new VertexComponentIdMapFunction(componentIdName));

      DataSet<EdgeComponentTuple3> edgeWithComps = graph.getEdgeIds()
          .leftOuterJoin(vertexComponentIds)
          .where(0)
          .equalTo(0)
          .with(new EdgeComponentIdJoinFunction());

      DataSet<Tuple3<Long, Integer, Integer>> tmpResult = edgeWithComps
          .leftOuterJoin(oneToManyVertexComponentIds)
          .where(2)
          .equalTo(1)
          .with(new AggregateBaseDeletedEdgesJoinFunction())
          .groupBy(0)
          .sum(1).and(Aggregations.SUM, 2)
          .filter(new FilterFunction<Tuple3<Long, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple3<Long, Integer, Integer> tuple) throws Exception {
              return tuple.f1 != 0;
            }
          });
      Utils.writeToFile(tmpResult, "rmEdgesPerCompAndEdgeCount");

      DataSet<Tuple3<Integer, Integer, Integer>> result = getAggCount(tmpResult);
      Utils.writeToFile(result, "rmEdgesCountAggregated");

      out.addTuples("removed edges, edges in component, count", result);
    }
  }

  @Deprecated
  public static DataSet<Tuple2<Long, Integer>> writeVertexComponentsToHDFS(
      Graph<Long, ObjectMap, ObjectMap> graph, final String compId, String prefix) {

    // single line per vertex
    DataSet<Tuple3<Long, Long, Integer>> vertexComponents = graph.getVertices()
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple3<Long, Long, Integer>>() {
          @Override
          public Tuple3<Long, Long, Integer> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Tuple3<>((long) vertex.getValue().get(Constants.HASH_CC),
                (long) vertex.getValue().get(compId), 1);
          }
        });

    DataSet<Tuple3<Long, Integer, Integer>> aggVertexComponents = vertexComponents
        .groupBy(1) // compId
        .sum(2)
        .and(Aggregations.MIN, 0) //hash cc
        .map(new MapFunction<Tuple3<Long, Long, Integer>, Tuple3<Long, Integer, Integer>>() {
          @Override
          public Tuple3<Long, Integer, Integer> map(Tuple3<Long, Long, Integer> idCompCountTuple) throws Exception {
            return new Tuple3<>(idCompCountTuple.f0, idCompCountTuple.f2, 1);
          }
        });

    return aggVertexComponents.project(0, 1);
  }

  public static DataSet<Tuple3<Integer, Integer, Integer>> getAggCount(DataSet<Tuple3<Long, Integer, Integer>> tmpResult) {
    return tmpResult
        .map(new MapFunction<Tuple3<Long, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
          @Override
          public Tuple3<Integer, Integer, Integer> map(Tuple3<Long, Integer, Integer> tuple) throws Exception {
            return new Tuple3<>(tuple.f1, tuple.f2, 1);
          }
        })
        .groupBy(0, 1)
        .sum(2);
  }

  /**
   * Create a string representation of a vertex for evaluation output.
   * @param vertex input vertex
   * @return resulting string
   */
  public static String toString(Vertex<Long, ObjectMap> vertex) {
    return toString(vertex, null);
  }

  public static ArrayList<Long> getVertexList(String dataset) {
//    ArrayList<Long> clusterList = Lists.newArrayList(1458L);//, 2913L);//, 4966L, 5678L);

    if (dataset.equals(Constants.GEO_FULL_NAME)) {
      // eval components cikm paper
      return Lists.newArrayList(457L, 442L, 583L, 172L, 480L, 22L, 531L, 190L, 128L, 488L,
          601L, 20L, 312L, 335L, 18L, 486L, 120L, 607L, 44L, 459L, 484L, 150L, 244L,
          522L, 320L, 294L, 256L, 140L, 324L, 98L, 396L, 542L, 50L, 533L, 492L, 148L, 152L, 524L, 248L,
          337L, 54L, 476L, 78L, 274L, 327L, 298L, 351L, 214L, 214L, 240L, 154L, 212L, 192L, 454L, 300L,
          258L, 467L, 478L, 345L, 347L, 272L, 394L, 264L, 198L, 116L, 286L, 38L, 361L, 230L, 373L, 232L,
          520L, 52L, 363L, 398L);
      //lake louise: 123L, 122L, 2060L, 1181L
    } else {
      return Lists.newArrayList(100972L, 121545L, 276947L, 235633L, 185488L, 100971L, 235632L, 121544L, 909033L);
    }
  }

  /**
   * Create a string representation of a vertex for evaluation output.
   * @param vertex input vertex
   * @param newCc needed if base vertices are observed
   * @return resulting string
   */
  public static String toString(Vertex<Long, ObjectMap> vertex, Long newCc) {
    String cc;
    if (newCc == null) {
      cc = vertex.getValue().containsKey(Constants.CC_ID)
          ? ", cc(" + vertex.getValue().get(Constants.CC_ID).toString() + ")" : "";
    } else {
      cc = ", finalCc(" + newCc.toString() + ")";
    }

    String type = vertex.getValue().containsKey(Constants.TYPE_INTERN)
        ? vertex.getValue().get(Constants.TYPE_INTERN).toString() : "";
    String label = Simplifiers
        .toLowerCase()
        .simplify(vertex.getValue().get(Constants.LABEL).toString());

    Double latitude = vertex.getValue().getLatitude();
    Double longitude = vertex.getValue().getLongitude();
    String latlon;

    String clusterVertices = "";
    if (vertex.getValue().containsKey(Constants.CL_VERTICES)) {
      clusterVertices = " clusterVertices: " + vertex.getValue().getVerticesList().toString();
    }

    String ontology = "";
    if (vertex.getValue().containsKey(Constants.ONTOLOGY)) {
      ontology = " source: " + vertex.getValue().getOntology();
    }
    if (vertex.getValue().containsKey(Constants.ONTOLOGIES)) {
      ontology = " sources: " + vertex.getValue().getOntologiesList().toString();
    }
    if (vertex.getValue().containsKey(Constants.DB_URL_FIELD)) {
      ontology = " uri: " + vertex.getValue().get(Constants.DB_URL_FIELD).toString();
    }

    if (latitude == null || longitude == null) {
      latlon = "NO geo";
    } else {
      BigDecimal lat = new BigDecimal(latitude);
      lat = lat.setScale(2, BigDecimal.ROUND_HALF_UP);
      BigDecimal lon = new BigDecimal(longitude);
      lon = lon.setScale(2, BigDecimal.ROUND_HALF_UP);

      latlon = "geo(" + lat + "|" + lon + ")";
    }
    return "##  (" + label + ": id(" + vertex.getId() + ")"
        + cc + ", type(" + type + "), " + latlon + clusterVertices + ontology + ")";
  }

  /**
   * For unclear types, generalize to the most common ancestor
   * @param vertexType input string
   * @return generalized type
   */
  public static String getShadingType(String vertexType) {
    if (TypeDictionary.TYPE_SHADINGS.containsKey(vertexType)
        || TypeDictionary.TYPE_SHADINGS.containsValue(vertexType)) {
      switch (vertexType) {
        case "School":
          vertexType = "ArchitecturalStructure";
          break;
        case "Island":
          vertexType = "Mountain";
          break;
        case "Settlement":
        case "Country":
          vertexType = "AdministrativeRegion";
          break;
      }
    }

    return vertexType;
  }

  /**
   * For unclear types, generalize to the most common ancestors
   * @param types input string
   * @return generalized type
   */
  public static Set<String> getShadingTypes(Set<String> types) {
    Set<String> result = Sets.newHashSet(types);
    for (String type : types) {
      String tmp = getShadingType(type);
      if (!tmp.equals(type)) {
        result.remove(type);
        result.add(tmp);
      }
    }

    return result;
  }

  /**
   * Remove non-words and write the value as lower case to the new object.
   * @param value input string
   * @return simplified value
   */
  public static String simplify(String value) {
    value = Simplifiers.removeAll("[\\(|,].*").simplify(value);
//    value = Simplifiers.removeNonWord().simplify(value);
    return Simplifiers.toLowerCase().simplify(value.trim());
  }

  /**
   * Get basic trigram string metric.
   * @return metric
   */
  public static StringMetric getBasicTrigramMetric() {
    return with(new CosineSimilarity<>())
        .tokenize(Tokenizers.qGram(3))
        .build();
  }

  public static StringMetric getTrigramMetricAndSimplifyStrings() {
    return with(new CosineSimilarity<>())
        .simplify(Simplifiers.removeAll("[\\(|,].*"))
//        .simplify(Simplifiers.replaceNonWord()) // TODO removeNonWord ??
        .simplify(Simplifiers.toLowerCase())
        .tokenize(Tokenizers.qGram(3))
        .build();
  }

  /**
   * Split a string to each of the contained long elements and return as a list.
   * @param input string
   * @return long list
   */
  public static List<Long> convertWsSparatedString(String[] input) {
    List<Long> result = Lists.newArrayList();
    for (String value : input) {
      result.add(Long.valueOf(value));
    }
    return result;
  }

  public static Long getHash(String input) {
    return HF.hashBytes(input.getBytes()).asLong();
  }

  public static String toLog(Vertex<Long, ObjectMap> vertex) {
    ObjectMap values = vertex.getValue();
    values.remove(Constants.TYPE);
    values.remove(Constants.DB_URL_FIELD);
    values.remove(Constants.COMP_TYPE);
    values.remove(Constants.TMP_TYPE);
    values.remove(Constants.VERTEX_OPTIONS);

    return vertex.toString();
  }

  public static String toLog(Edge<Long, ObjectMap> edge) {
    return edge.getSource().toString()
        .concat("<->").concat(edge.getTarget().toString())
        .concat(": ").concat(edge.getValue().getEdgeSimilarity().toString());
  }


  private static class VertexComponentIdMapFunction implements MapFunction<Vertex<Long,ObjectMap>,
      VertexComponentTuple2> {
    private final String component;

    public VertexComponentIdMapFunction(String component) {
      this.component = component;
    }

    @Override
    public VertexComponentTuple2 map(Vertex<Long, ObjectMap> vertex) throws Exception {
      return new VertexComponentTuple2(vertex.getId(), (long) vertex.getValue().get(component));
    }
  }

  private static class EdgeComponentIdJoinFunction implements JoinFunction<Tuple2<Long,Long>,
      VertexComponentTuple2, EdgeComponentTuple3> {
    @Override
    public EdgeComponentTuple3 join(Tuple2<Long, Long> left, VertexComponentTuple2 right) throws Exception {
      return new EdgeComponentTuple3(left.f0, left.f1, right.getComponentId());
    }
  }

  private static class AggregateBaseDeletedEdgesJoinFunction
      implements JoinFunction<EdgeComponentTuple3, VertexComponentTuple2, Tuple3<Long, Integer, Integer>> {
    @Override
    public Tuple3<Long, Integer, Integer> join(EdgeComponentTuple3 left,
                                               VertexComponentTuple2 right) throws Exception {
      if (right == null) {
        return new Tuple3<>(left.getComponentId(), 0, 1);
      } else {
        if ((long) left.getSourceId() == right.getVertexId()
            || (long) left.getTargetId() == right.getVertexId()) {
          return new Tuple3<>(left.getComponentId(), 1, 1);
        } else {
          return new Tuple3<>(left.getComponentId(), 0, 1);
        }
      }
    }
  }
}