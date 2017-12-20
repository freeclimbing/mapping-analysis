package org.mappinganalysis.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.mappinganalysis.io.output.ExampleOutput;
import org.mappinganalysis.model.EdgeComponentTuple3;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.VertexComponentTuple2;
import org.mappinganalysis.model.functions.CharSet;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.simplifiers.Simplifiers;
import org.simmetrics.tokenizers.Tokenizers;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static org.simmetrics.builders.StringMetricBuilder.with;

public class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class);

  private static final HashFunction HF = Hashing.md5();

  /**
   * Write any dataset to disk, not working currently, old.
   */
  @Deprecated
  public static <T> void writeToFile(DataSet<T> data, String outDir) {
    if (Constants.VERBOSITY.equals(Constants.DEBUG)) {
      data.writeAsFormattedText(Constants.INPUT_PATH + "output/" + outDir,
          FileSystem.WriteMode.OVERWRITE,
          new DataSetTextFormatter<>());
    }
  }

  /**
   * Get the hash map value having the highest count of occurrence.
   * For string value property, if count is equal, a longer string is preferred.
   * @param map containing value options with count of occurrence
   * @return resulting value
   */
  public static <T> T getFinalValue(HashMap<T, Integer> map) {
    if (map.isEmpty()) {
      return null;
    }
    Map.Entry<T, Integer> finalEntry = null;
    map = Utils.sortByValue(map);

    for (Map.Entry<T, Integer> entry : map.entrySet()) {
      if (finalEntry == null || Ints.compare(entry.getValue(), finalEntry.getValue()) > 0) {
        finalEntry = entry;
      } else if (entry.getKey() instanceof String
          && Ints.compare(entry.getValue(), finalEntry.getValue()) >= 0) {
        String labelKey = entry.getKey().toString();
        if (labelKey.length() > finalEntry.getKey().toString().length()) {
          finalEntry = entry;
        }
      }
    }

    checkArgument(finalEntry != null, "Entry must not be null");
    return finalEntry.getKey();
  }

  /**
   * Check for null and other anomaly values.
   */
  public static Boolean isSane(String value) {
    return !(value == null
        || value.equals(Constants.NO_LABEL_FOUND)
        || value.equals(Constants.NO_VALUE)
        || value.equals(Constants.CSV_NO_VALUE));
  }

  /**
   * Check for null and other anomaly values.
   */
  public static Boolean isSaneInt(Integer value) {
    return !(value == null
        || value == Constants.EMPTY_INT);
  }

  /**
   * Sort a hash map by descending values
   */
  public static <T> HashMap<T, Integer> sortByValue(HashMap<T, Integer> map) {
    HashMap<T, Integer> result = new LinkedHashMap<>();
    map.entrySet()
        .stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .forEachOrdered(value -> result.put(value.getKey(), value.getValue()));

    return result;
  }

//  public void setBlockingKey(BlockingStrategy strategy, String mode, String label) {
//    map.put(Constants.BLOCKING_LABEL,
//        Utils.getBlockingKey(strategy, mode, label));
//  }
  /**
   * Use outside of ObjectMap:
   * Based on a blocking strategy and based on data domain
   * set the blocking label for a single instance.
   * @param strategy BlockingStrategy {@see BlockingStrategy}
   */
  public static String getBlockingKey(
      BlockingStrategy strategy,
      String bMode,
      String label) {
    if (strategy.equals(BlockingStrategy.STANDARD_BLOCKING)) {
      if (bMode.equals(Constants.GEO)) {
//        LOG.info("sbs gL: " + getLabel());
//        LOG.info("sbs map: " + getMap());

        return Utils.getGeoBlockingLabel(label);
      } else if (bMode.equals(Constants.MUSIC)) {
//        LOG.info("music put blocking label");

        return Utils.getMusicBlockingLabel(label);
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    } else if (strategy.equals(BlockingStrategy.NO_BLOCKING)) {

      return Constants.NO_VALUE;
    } else {
      throw new IllegalArgumentException("Unsupported strategy: " + strategy);
    }
  }

  public static double getExactDoubleResult(double value) {
    return new BigDecimal(value)
        .setScale(6, BigDecimal.ROUND_HALF_UP)
        .doubleValue();
  }

  public static double getExactDoubleResult(double dividend, long divisor) {
    return new BigDecimal(dividend / divisor)
        .setScale(6, BigDecimal.ROUND_HALF_UP)
        .doubleValue();
  }

  public static class DataSetTextFormatter<V>
      implements TextOutputFormat.TextFormatter<V> {
    @Override
    public String format(V v) {
      return v.toString();
    }
  }

  public static Double computeWithMetric(StringMetric metric, String first, String second) {
    double similarity = metric.compare(first.trim().toLowerCase(), second.trim().toLowerCase());
    BigDecimal tmpResult = new BigDecimal(similarity);

    return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
  }

  public static MergeGeoTuple isOnlyOneValidGeoObject(MergeGeoTuple left, MergeGeoTuple right) {
    if (isValidGeoObject(left) && !isValidGeoObject(right)) {
      return left;
    } else if (!isValidGeoObject(left) && isValidGeoObject(right)) {
      return right;
    } else {
      return null;
    }
  }

  public static boolean isValidGeoObject(MergeGeoTuple triplet) {
    if (triplet.getLatitude() == null || triplet.getLongitude() == null) {
      return Boolean.FALSE;
    }
    return isValidLatitude(triplet.getLatitude()) && isValidLongitude(triplet.getLongitude());
  }

  public static boolean isValidGeoObject(Double latitude, Double longitude) {
    if (latitude == null || longitude == null) {
      return Boolean.FALSE;
    }
    return isValidLatitude(latitude) && isValidLongitude(longitude);
  }

  public static boolean isValidLatitude(Double latitude) {
    return latitude != null
        && Doubles.compare(latitude, 90) <= 0 && Doubles.compare(latitude, -90) >= 0;
  }

  public static boolean isValidLongitude(Double longitude) {
    return longitude != null
        && Doubles.compare(longitude, 180) <= 0 && Doubles.compare(longitude, -180) >= 0;
  }


  /**
   * Gets geo distance for lat/lon on source/target. Validity check and normalization to max geo distance.
   * Care for max distance value from constants file
   */
  public static Double getGeoSimilarity(Double latLeft, Double lonLeft, Double latRight, Double lonRight) {
    if (isValidGeoObject(latLeft, lonLeft)
        && isValidGeoObject(latRight, lonRight)) {
      Double distance = GeoDistance.distance(latLeft, lonLeft, latRight, lonRight);

      if (distance >= Constants.MAXIMAL_GEO_DISTANCE) {
        return 0D;
      } else {
        double tmp = 1D - (distance / Constants.MAXIMAL_GEO_DISTANCE);
        BigDecimal tmpResult = new BigDecimal(tmp);

        return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
      }
    } else {
      return null;
    }
  }

  /**
   * Basic trigram similarity for 2 labels.
   */
  public static Double getLabelSimilarity(String left, String right) {
    Preconditions.checkNotNull(left);
    Preconditions.checkNotNull(right);

    double similarity = getTrigramMetricAndSimplifyStrings()
        .compare(left.toLowerCase().trim(), right.toLowerCase().trim());
    BigDecimal tmpResult = new BigDecimal(similarity);

    return tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
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
          .filter(tuple -> tuple.f1 != 0)
          .returns(new TypeHint<Tuple3<Long, Integer, Integer>>() {});


      Utils.writeToFile(tmpResult, "rmEdgesPerCompAndEdgeCount");

      DataSet<Tuple3<Integer, Integer, Integer>> result = getAggCount(tmpResult);
      Utils.writeToFile(result, "rmEdgesCountAggregated");

      out.addTuples("removed edges, edges in component, count", result);
    }
  }

  public static DataSet<Tuple3<Integer, Integer, Integer>> getAggCount(
      DataSet<Tuple3<Long, Integer, Integer>> tmpResult) {
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
    if (vertex.getValue().containsKey(Constants.DATA_SOURCE)) {
      ontology = " source: " + vertex.getValue().getDataSource();
    }
    if (vertex.getValue().containsKey(Constants.DATA_SOURCES)) {
      ontology = " sources: " + vertex.getValue().getDataSourcesList().toString();
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
   * Get type similarity for two sets of type strings,
   * indirect type shading sim is also computed.
   *
   * TODO rework if type shading sim is <1
   * TODO inefficient always to check type shadings
   */
  public static double getTypeSim(Set<String> srcTypes, Set<String> trgTypes) {
//    if (srcTypes.contains(Constants.NO_TYPE) || trgTypes.contains(Constants.NO_TYPE)) {
//      return 0;
//    }
    for (String srcType : srcTypes) {
      if (trgTypes.contains(srcType)) {
        return 1;
      } else {
        for (String trgType : trgTypes) {
          double check = checkTypeShadingSimilarity(srcType, trgType);
          if (Doubles.compare(check, 0d) != 0) {
            return check;
          }
        }
      }
    }
    return 0;
  }

  /**
   * return double because of option to reduce the result value according to shading type sim (default: 1)
   */
  private static double checkTypeShadingSimilarity(String srcType, String trgType) {
    if (TypeDictionary.TYPE_SHADINGS.containsKey(srcType)
        && TypeDictionary.TYPE_SHADINGS.get(srcType).equals(trgType)
        || TypeDictionary.TYPE_SHADINGS.containsKey(trgType)
        && TypeDictionary.TYPE_SHADINGS.get(trgType).equals(srcType)) {
      return Constants.SHADING_TYPE_SIM;
    } else {
      return 0d;
    }
  }

  /**
   * If src or trg has "no_type" type, true is returned.
   */
  public static boolean hasEmptyType(Set<String> srcType, Set<String> trgType) {
    return srcType.contains(Constants.NO_TYPE) || trgType.contains(Constants.NO_TYPE);
  }

  /**
   * music blocking
   * Get the first 4 chars of string.
   *
   * Check dataset for blocked prefix values!
   */
  public static String getMusicBlockingLabel(String label) {
    label = label.toLowerCase();
    String tmp = label;
    int blockingLength = 4;

    Set<String> blockElements = Sets.newHashSet
        // artist first common start words
//        ("the ", "john", "joha", "nn s", "ebas", "tian", " bac", "fran", "geor", "wolf", "gang",
//            "chri", "unkn", "own ", "mich", " ama", "deus", " moz", "art ");
        // titles + common start words, TITLE attribute
        ("the ", "001-", "003-", "005-", "002-", "004-",
        "007-", "006-", "009-", "008-", "010-", "011-", "012-", "013-", "014-", "015-", "016-", "017-",
        "018-", "019-", "020-", "love", "you ", "some", "all ", "don'", "symp");
        // numbers only, TITLE attribute
//            ("001-", "003-", "005-", "002-", "004-",
//        "007-", "006-", "009-", "008-", "010-", "011-", "012-", "013-", "014-", "015-", "016-", "017-",
//        "018-", "019-", "020-");//, "love", "you ", "some", "all ");//, "don'", "symp", "no_va") ;

    String blockedLabel = updateBlockedLabel(label, blockingLength);

    while (blockElements.contains(blockedLabel)) {
      label = label.substring(blockedLabel.length());
      blockedLabel = updateBlockedLabel(label, blockingLength);
    }

    if (label.length() >= blockingLength) {
      label = label.substring(0, blockingLength);
    }

    if (label.equals("")) {
//      System.out.println(label + " --- " + tmp);
      if (tmp.length() >= blockingLength) {
        tmp = tmp.substring(0, blockingLength);
      }
//      LOG.info(tmp);
      return tmp;
    }
    return label;
  }

  /**
   * replace non-word characters and special characters by white spaces for idf
   */
  public static String createArtistTitleAlbum(String simpleArtistTitleAlbum) {
    simpleArtistTitleAlbum = simpleArtistTitleAlbum
        .replaceAll("[\\W\\p{Punct}]", " ");

    Pattern regex = Pattern
        .compile("([a-z])([A-Z])");
    Matcher matcher = regex.matcher(simpleArtistTitleAlbum);

    while (matcher.find()) {
      simpleArtistTitleAlbum = matcher.replaceAll("$1 $2");
//      System.out.println("replaced: " + s);

//      System.out.println("ata: " + artistTitleAlbum);
    }

    return simpleArtistTitleAlbum;
  }

  /**
   * Blocking key string, no hard replacement of non words and special characters.
   * @return simple concatenated album title artist
   */
  public static String createSimpleArtistTitleAlbum(Vertex<Long, ObjectMap> value) {
    String artistTitleAlbum = Constants.EMPTY_STRING;
    String artist = value.getValue().getArtist();
    if (!artist.equals(Constants.CSV_NO_VALUE)) {
      artistTitleAlbum = artist;
    }
    String label = value.getValue().getLabel();
    if (!label.equals(Constants.CSV_NO_VALUE)) {
      if (artistTitleAlbum.equals(Constants.EMPTY_STRING)) {
        artistTitleAlbum = label;
      } else {
        artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(label);
      }
    }
    String album = value.getValue().getAlbum();
    if (!album.equals(Constants.CSV_NO_VALUE)) {
      if (artistTitleAlbum.equals(Constants.EMPTY_STRING)) {
        artistTitleAlbum = album;
      } else {
        artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(album);
      }
    }

    String tmp = artistTitleAlbum;
    artistTitleAlbum = artistTitleAlbum
//        .replaceAll("\\W", "")
        .replaceAll("\\p{Punct}", "");

    if (artistTitleAlbum.equals(Constants.EMPTY_STRING)) {
      System.out.println(tmp);
    }

    return artistTitleAlbum;
  }

  /**
   * Get label substring of according length
   */
  private static String updateBlockedLabel(String label, int blockingLength) {
    if (label.length() >= blockingLength) {
      return label.substring(0, blockingLength);
    } else {
      return label;
    }
  }

  /**
   * "old" geo blocking
   * Get the first 3 chars of string. If label is shorter, fill up with '#'.
   */
  public static String getGeoBlockingLabel(String label) {
    if (label.length() < 3) {
      label += StringUtils.repeat("#", 3 - label.length());
    }

    label = label.substring(0, 3).toLowerCase();
    label = label.replaceAll("[^a-zA-Z0-9#]+","#");

    // needed for chinese chars for example
    if (label.length() < 3) {
      label += StringUtils.repeat("#", 3 - label.length());
    }

    return label;
  }

  /**
   * Remove non-words and write the value as lower case to the new object.
   * @param value input string
   * @return simplified value
   */
  public static String simplify(String value) {
    value = Simplifiers.removeAll("[\\(|,].*").simplify(value);

    return value.toLowerCase().trim();
  }

  public static StringMetric getTrigramMetricAndSimplifyStrings() {
    return with(new CosineSimilarity<>())
        .simplify(Simplifiers.removeAll("[\\\\(|,].*"))
        .simplify(Simplifiers.removeAll("\\s"))
        //.simplify(Simplifiers.replaceNonWord()) // TODO removeNonWord ??
        .simplify(Simplifiers.toLowerCase())
        .tokenize(Tokenizers.qGramWithPadding(3))
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

  /**
   * Get all lowercase trigrams as char set for a given String
   */
  public static CharSet getUnsortedTrigrams(String input) {
    CharSet result = new CharSet();
    input = input.toLowerCase();

    for (int i = 0; i < input.length() - 2; i++) {
      char[] chars = new char[3];
      chars[0] = input.charAt(i);
      chars[1] = input.charAt(i + 1);
      chars[2] = input.charAt(i + 2);

      result.add(chars);
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