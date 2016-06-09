package org.mappinganalysis.utils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.EdgeComponentTuple3;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.VertexComponentTuple2;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.simplifiers.Simplifiers;
import org.simmetrics.tokenizers.Tokenizers;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static org.simmetrics.builders.StringMetricBuilder.with;

public class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class);

  /**
   * Database property names.
   */
  private static String DB_NAME = "";
  public static String INPUT_DIR;
  public static String VERBOSITY;
  public static boolean IS_SIMSORT_ENABLED;
  public static Double MIN_CLUSTER_SIM;
  public static Double MIN_LABEL_PRIORITY_SIM;
  public static Double MIN_SIMSORT_SIM;
  public static boolean IGNORE_MISSING_PROPERTIES;
  public static boolean IS_RESTRICT_ACTIVE;
  public static String PRE_CLUSTER_STRATEGY;
  public static boolean IS_LINK_FILTER_ACTIVE;

  public static final String DEBUG = "debug";
  public static final String INFO = "info";
  public static final String LESS = "less";

  public static final String LL_DB_NAME = "linklion_links_9_2015";
  public static final String BIO_DB_NAME = "bioportal_mappings_11_08_2015";
  public static final String GEO_PERFECT_DB_NAME = "hartung_perfect_geo_links";
  public static final String LL_FULL_NAME = "dbURLfullLinkLion";
  public static final String GEO_FULL_NAME = "dbURLfull";

  /**
   * Modes restricting entity enrichment.
   */
  public static final String MODE_LAT_LONG_TYPE = "modeLatLongType";
  public static final String MODE_LABEL = "modeLabel";
  public static final String MODE_TYPE = "modeType";
  public static final String MODE_ALL = "modeAll";

  public static final String DB_PROPERY_FILE_NAME = "db";

  public static final String DB_URL_FIELD = "url";
  public static final String DB_ID_FIELD = "id";
  public static final String DB_ATT_VALUE_TYPE = "attValueType";
  public static final String DB_ONTID_FIELD = "ontID_fk";
  /**
   * Command line option names.
   */
  public static final String CMD_LL = "linklion";
  public static final String CMD_GEO = "geo";
  public static final String DEFAULT_VALUE = "default";
  public static final String PROCESSING_MODE_NEXT_TGB = "next-tgb";

  /**
   * Accumulators
   */
  public static final String PROP_COUNT_ACCUMULATOR = "prop-count";
  public static final String EDGE_COUNT_ACCUMULATOR = "edge-count";
  public static final String VERTEX_COUNT_ACCUMULATOR = "vertex-count";
  public static final String BASE_VERTEX_COUNT_ACCUMULATOR = "vertex-count";
  public static final String TYPES_COUNT_ACCUMULATOR = "types-count";

  public static final String PREPROC_LINK_FILTER_ACCUMULATOR = "links-filtered-counter";
  public static final String FILTERED_LINKS_ACCUMULATOR = "links-filtered";
  public static final String RESTRICT_EDGE_COUNT_ACCUMULATOR = "restrict-count";

  public static final String SIMSORT_EXCLUDE_FROM_COMPONENT_ACCUMULATOR = "exclude-from-component-counter";
  public static final String EDGE_EXCLUDE_ACCUMULATOR = "edge-exclude-counter";
  public static final String REFINEMENT_MERGE_ACCUMULATOR = "merged-cluster-counter";
  public static final String EXCLUDE_VERTEX_ACCUMULATOR = "exclude-vertex-counter";
  public static final String REPRESENTATIVE_ACCUMULATOR = "representative-counter";
  public static final String VERTEX_OPTIONS = "vertex-options";

  public static final String AGG_PREFIX = "aggregated-";
  public static final String AGG_VALUE_COUNT = "aggValueCount";

  /**
   * DB fields
   */
  public static final String DB_CCID_FIELD = "ccID";
  public static final String DB_CONCEPTID_FIELD = "conceptID";
  public static final String DB_CC_TABLE = "connectedComponents";

  /**
   * similarity default values.
   */
  public static final Double MAXIMAL_GEO_DISTANCE = 150000D;
  public static final Double SHADING_TYPE_SIM = 1D;
  public static final String SIM_GEO_LABEL_STRATEGY = "geo-label";
  /**
   * DB attName for gn type detail information
   */
  public static final String GN_TYPE_DETAIL = "typeDetail";
  /**
   * DB column name for 'rdf:type'/..., 'geo:lat/lon', 'rdfs:label'/'gn:name'/... field.
   */
  public static final String TYPE = "type";
  public static final String LAT = "lat";
  public static final String LON = "lon";
  public static final String LABEL = "label";
  /**
   * DB column name for GeoNames second type value field.
   */
  public static final String TYPE_INTERN = "typeIntern";
  public static final String TMP_TYPE = "tmpType" ;
  public static final String COMP_TYPE = "compType";
  /**
   * temp values for missing values
   */
  public static final String NO_LABEL_FOUND = "no_label_found";
  public static final String NO_TYPE = "no_type";
  /**
   * Field name for sim value names
   */
  public static final String SIM_TYPE = "typeMatch";
  public static final String SIM_TRIGRAM = "trigramSim";
  public static final String SIM_DISTANCE = "distance";
  public static final String AGGREGATED_SIM_VALUE = "aggSimValue";
  public static final String VERTEX_AGG_SIM_VALUE = "vertexAggSimValue";
  public static final String VERTEX_STATUS = "vertexStatus";
  public static final Double DEFAULT_VERTEX_SIM = -1D;
  public static final Double DEACTIVATE_VERTEX = -2D;
  public static final boolean VERTEX_STATUS_ACTIVE = Boolean.TRUE;
  /**
   * field name for connected component ID
   */
  public static final String CC_ID = "ccId";
  public static final String VERTEX_ID = "vertexId";
  public static final String HASH_CC = "hashCc";
  public static final String OLD_HASH_CC = "oldHashCc";
  public static final String REFINE_ID = "refineId";

  /**
   * property name in cluster representative where clustered vertivces are put into.
   */
  public static final String CL_VERTICES = "clusteredVertices";
  /**
   * source name of resource. internal name for db field ontID_fk
   */
  public static final String ONTOLOGY = "ontology";
  public static final String ONTOLOGIES = "ontologies";

  /**
   * source namespaces
   */
  public static final String DBP_NS = "http://dbpedia.org/";
  public static final String GN_NS = "http://sws.geonames.org/";
  public static final String LGD_NS = "http://linkedgeodata.org/";
  public static final String NYT_NS = "http://data.nytimes.com/";
  public static final String FB_NS = "http://rdf.freebase.com/";


  private static boolean DB_UTF8_MODE = false;
  private static final HashFunction HF = Hashing.md5();

  public static HttpURLConnection openUrlConnection(URL url) {
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
		
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ conn.getResponseCode());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static Connection openDbConnection() throws SQLException {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    ResourceBundle prop = ResourceBundle.getBundle(Utils.DB_PROPERY_FILE_NAME,
        Locale.getDefault(), loader);

    Connection con;
    String url = prop.getString("dbURL");
    LOG.info(url);
    url += DB_NAME;
    if (DB_UTF8_MODE) {
      url += "?useUnicode=true&characterEncoding=utf-8";
    }
    con = DriverManager.getConnection(url, prop.getString("user"), prop.getString("pw"));

    return con;
	}

  /**
   * Custom API key for Freebase data retrieval - 100.000 queries/day possible
   * @return api key
   */
  public static String getGoogleApiKey() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    ResourceBundle prop = ResourceBundle.getBundle(Utils.DB_PROPERY_FILE_NAME,
        Locale.getDefault(), loader);

    return prop.getString("apiKey");
  }
  /**
   * Get a MongoDB (currently only working on wdi05 -> localhost)
   * @param dbName mongo db name
   * @return MongoDatabase
   */
  public static MongoDatabase getMongoDatabase(String dbName) {
    MongoClient client = new MongoClient("localhost", 27017);

    return client.getDatabase(dbName);
  }

  public static <T> void writeToHdfs(DataSet<T> data, String outDir) {
    if (VERBOSITY.equals(DEBUG)) {
      data.writeAsFormattedText(INPUT_DIR + "output/" + outDir,
          FileSystem.WriteMode.OVERWRITE,
          new DataSetTextFormatter<T>());
    }
  }

  public static class DataSetTextFormatter<V> implements
      TextOutputFormat.TextFormatter<V> {
    @Override
    public String format(V v) {
      return v.toString();
    }
  }

  public static void writeRemovedEdgesToHDFS(Graph<Long, ObjectMap, ObjectMap> graph,
                                             DataSet<VertexComponentTuple2> oneToManyVertexComponentIds,
                                             String componentIdName, ExampleOutput out) {
    if (VERBOSITY.equals(DEBUG)) {
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
      Utils.writeToHdfs(tmpResult, "rmEdgesPerCompAndEdgeCount");

      DataSet<Tuple3<Integer, Integer, Integer>> result = getAggCount(tmpResult);
      Utils.writeToHdfs(result, "rmEdgesCountAggregated");

      out.addTuples("removed edges, edges in component, count", result);
    }
  }

  public static DataSet<Tuple2<Long, Integer>> writeVertexComponentsToHDFS(
      Graph<Long, ObjectMap, ObjectMap> graph, final String compId, String prefix) {

    // single line per vertex
    DataSet<Tuple3<Long, Long, Integer>> vertexComponents = graph.getVertices()
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple3<Long, Long, Integer>>() {
          @Override
          public Tuple3<Long, Long, Integer> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Tuple3<>((long) vertex.getValue().get(Utils.HASH_CC),
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
//    Utils.writeToHdfs(aggVertexComponents, prefix + "AggVertexComps");

    // too much aggregated, not needed yet
//    DataSet<Tuple2<Integer, Integer>> aggAggCount = aggVertexComponents.groupBy(1).sum(2).project(1, 2);
//    Utils.writeToHdfs(aggAggCount, prefix + "AggAggVertexComps");

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

    if (dataset.equals(Utils.GEO_FULL_NAME)) {
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
      cc = vertex.getValue().containsKey(CC_ID)
          ? ", cc(" + vertex.getValue().get(CC_ID).toString() + ")" : "";
    } else {
      cc = ", finalCc(" + newCc.toString() + ")";
    }

    String type = vertex.getValue().containsKey(TYPE_INTERN)
        ? vertex.getValue().get(TYPE_INTERN).toString() : "";
    String label = Simplifiers
        .toLowerCase()
        .simplify(vertex.getValue().get(LABEL).toString());

    Double latitude = vertex.getValue().getLatitude();
    Double longitude = vertex.getValue().getLongitude();
    String latlon;

    String clusterVertices = "";
    if (vertex.getValue().containsKey(CL_VERTICES)) {
      clusterVertices = " clusterVertices: " + vertex.getValue().getVerticesList().toString();
    }

    String ontology = "";
    if (vertex.getValue().containsKey(ONTOLOGY)) {
      ontology = " source: " + vertex.getValue().get(ONTOLOGY).toString();
    }
    if (vertex.getValue().containsKey(ONTOLOGIES)) {
      ontology = " sources: " + vertex.getValue().get(ONTOLOGIES).toString();
    }
    if (vertex.getValue().containsKey(DB_URL_FIELD)) {
      ontology = " uri: " + vertex.getValue().get(DB_URL_FIELD).toString();
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
   * Set UTF8 Mode for database access
   * @param value true
   */
  public static void setUtf8Mode(boolean value) {
    DB_UTF8_MODE = value;
  }

  /**
   * Open a db connection to a specified database name.
   * @param dbName db name
   * @return db connection
   * @throws SQLException
   */
  public static Connection openDbConnection(String dbName) throws SQLException {
    DB_NAME = dbName;
    return openDbConnection();
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
        case "Mountain":
          vertexType = "Island";
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
   * Geo coords helper function
   * @param latOrLon one of the two geocoordinates
   * @return single double value
   */
  public static Double getGeoDouble(Object latOrLon) {
    // TODO how to handle multiple values in lat/lon correctly?
    if (latOrLon instanceof Set) {
      return (Double) Iterables.get((Set) latOrLon, 0);
    } else {
      return (Double) latOrLon;
//      return Doubles.tryParse(latOrLon.toString());
    }
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
    values.remove(Utils.TYPE);
    values.remove(Utils.DB_URL_FIELD);
    values.remove(Utils.COMP_TYPE);
    values.remove(Utils.TMP_TYPE);
    values.remove(Utils.VERTEX_OPTIONS);

    return vertex.toString();
  }

  public static String toLog(Edge<Long, ObjectMap> edge) {
    return edge.getSource().toString()
        .concat("<->").concat(edge.getTarget().toString())
        .concat(": ").concat(edge.getValue().get(Utils.AGGREGATED_SIM_VALUE).toString());
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