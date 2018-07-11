package org.mappinganalysis.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.mappinganalysis.graph.utils.GradoopEdgeToGellyEdgeMapper;
import org.mappinganalysis.graph.utils.GradoopToGellyEdgeJoinFunction;
import org.mappinganalysis.graph.utils.GradoopToObjectMapVertexMapper;
import org.mappinganalysis.model.GeoCode;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CharSet;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.metrics.JaroWinkler;
import org.simmetrics.simplifiers.Simplifiers;
import org.simmetrics.tokenizers.Tokenizers;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static org.simmetrics.builders.StringMetricBuilder.with;

public class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class);
  private static final HashFunction HF = Hashing.md5();

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

  @SafeVarargs
  public static <T> HashSet<T> merge(Collection<? extends T>... collections) {
    HashSet<T> newSet = new HashSet<>();
    for (Collection<? extends T> collection : collections)
      newSet.addAll(collection);

    return newSet;
  }

  public static String getOutputSuffix(double threshold) {
    Double tmp = threshold * 100;

    return String.valueOf(tmp.intValue());
  }

  public static LogicalGraph getGradoopGraph(String graphPath, ExecutionEnvironment env) {
    final String graphHeadFile  = graphPath.concat("graphHeads.json");
    final String vertexFile     = graphPath.concat("vertices.json");
    final String edgeFile       = graphPath.concat("edges.json");

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
    JSONDataSource dataSource = new JSONDataSource(graphHeadFile, vertexFile, edgeFile, config);

    return dataSource.getLogicalGraph();
  }

  public static Graph<Long, ObjectMap, NullValue> getInputGraph(
      LogicalGraph logicalGraph, String domain, ExecutionEnvironment env) {
    // get gelly vertices
    DataSet<Vertex<Long, ObjectMap>> vertices = logicalGraph
        .getVertices()
        .map(new GradoopToObjectMapVertexMapper(domain));

    // get gelly edges
    DataSet<Edge<Long, NullValue>> edges = logicalGraph.getEdges()
        .leftOuterJoin(logicalGraph.getVertices())
        .where(new SourceId<>())
        .equalTo(new Id<>())
        .with(new GradoopToGellyEdgeJoinFunction(0))
        .leftOuterJoin(logicalGraph.getVertices())
        .where(new TargetId<>())
        .equalTo(new Id<>())
        .with(new GradoopToGellyEdgeJoinFunction(1))
        .map(new GradoopEdgeToGellyEdgeMapper());

    return Graph.fromDataSet(vertices, edges, env);
  }

  /**
   * Use outside of ObjectMap:
   * Based on a blocking strategy and based on data domain
   * set the blocking label for a single instance.
   * @param strategy BlockingStrategy {@see BlockingStrategy}
   */
  public static String getBlockingKey(
      BlockingStrategy strategy,
      String bMode,
      String label,
      int blockingLength) {
    if (strategy == BlockingStrategy.STANDARD_BLOCKING
        || strategy == BlockingStrategy.BLOCK_SPLIT) {
      if (bMode.equals(Constants.GEO)) {

        return Utils.getGeoBlockingLabel(label);
      } else if (bMode.equals(Constants.MUSIC) || bMode.equals(Constants.NC)) {

        return Utils.getMusicBlockingLabel(label, blockingLength);
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    } else if (strategy.equals(BlockingStrategy.NO_BLOCKING)) { // DUMMY

      return Constants.NO_VALUE;
    } else if (strategy.equals(BlockingStrategy.LSH_BLOCKING)) { // DUMMY

      return Constants.NO_VALUE;
    } else {
      throw new IllegalArgumentException("Unsupported strategy: " + strategy);
    }
  }

  public static String getNcBlockingLabel(String name, String surname) {

    while (surname.length() <= 1) {
      surname = surname.concat(Constants.WHITE_SPACE);
    }
    while (name.length() <= 1) {
      name = name.concat(Constants.WHITE_SPACE);
    }

    return name.substring(0, 2).concat(surname.substring(0, 2));
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

  /**
   * Get blocking short name for logging.
   */
  public static String getShortBlockingStrategy(BlockingStrategy strategy) {
    if (strategy == BlockingStrategy.STANDARD_BLOCKING) {
      return Constants.SB;
    } else if (strategy == BlockingStrategy.BLOCK_SPLIT) {
      return Constants.BS;
    } else if (strategy == BlockingStrategy.LSH_BLOCKING) {
      return Constants.LSHB;
    } else {
      throw new IllegalArgumentException("Unsupported blocking strategy: " + strategy);
    }
  }

  public static HashMap<String, GeoCode> addGeoToMap(
      HashMap<String, GeoCode> geoMap,
      Vertex<Long, ObjectMap> vertex) {
    if (vertex.getValue().hasGeoPropertiesValid()) {
      Double latitude = vertex.getValue().getLatitude();
      Double longitude = vertex.getValue().getLongitude();

      if (vertex.getValue().containsKey(Constants.DATA_SOURCE)) {
        geoMap.put(vertex.getValue().getDataSource(),
            new GeoCode(latitude, longitude));
      } else if (vertex.getValue().containsKey(Constants.DATA_SOURCES)) {
        for (String value : vertex.getValue().getDataSourcesList()) {
          geoMap.put(value, new GeoCode(latitude, longitude));
        }
      }
    }
    return geoMap;
  }

  public static ObjectMap handleMusicProperties(
      Vertex<Long, ObjectMap> pri,
      Vertex<Long, ObjectMap> min) {
    ObjectMap priority = pri.getValue();
    ObjectMap minor = min.getValue();


    //Constants.ALBUM
    Boolean saneMinAlbum = isSane(minor.getAlbum());
    if (isSane(priority.getAlbum())) {
      if (saneMinAlbum) {
        String album = priority.getAlbum().length() >= minor.getAlbum().length()
            ? priority.getAlbum() : minor.getAlbum();
        priority.setAlbum(album);
      }
    } else {
      if (saneMinAlbum) {
        priority.setAlbum(minor.getAlbum());
      }
    }

    //Constants.ARTIST, priority, minor);
    Boolean saneMinArtist = isSane(minor.getArtist());
    if (isSane(priority.getArtist())) {
      if (saneMinArtist) {
        String artist = priority.getArtist().length() >= minor.getArtist().length()
            ? priority.getArtist() : minor.getArtist();
        priority.setArtist(artist);
      }
    } else {
      if (saneMinArtist) {
        priority.setArtist(minor.getArtist());
      }
    }

    //Constants.NUMBER, priority, minor);
    if (!isSane(priority.getNumber())
        && isSane(minor.getNumber())) {
      priority.setNumber(minor.getNumber());
    }

    if (!isSane(priority.getLanguage())
        && isSane(minor.getLanguage())) {
      priority.setLanguage(minor.getLanguage());
    }

    //Constants.YEAR, priority, minor);
    if (!isSaneInt(priority.getYear())
        && isSaneInt(minor.getYear())) {
      priority.setYear(minor.getYear());
    }

    //Constants.LENGTH, priority, minor);
    if (!isSaneInt(priority.getLength())
        && isSaneInt(minor.getLength())) {
      priority.setLength(minor.getLength());
    }

    return priority;
  }

  public static ObjectMap handleGeoProperties(
      Vertex<Long, ObjectMap> priorities,
      Vertex<Long, ObjectMap> minorities) {
    HashMap<String, GeoCode> geoMap = Maps.newHashMap();
    geoMap = addGeoToMap(geoMap, priorities);
    geoMap = addGeoToMap(geoMap, minorities);
    priorities.getValue().setGeoProperties(geoMap);

//    priorities.getValue().addTypes(
//        Constants.TYPE_INTERN, minorities.getValue().getTypesIntern());

    return priorities.getValue();
  }

  /**
   * quick and dirty get an empty set of edges, how to fix?
   */
  public static DataSet<Edge<Long, NullValue>> getFakeEdges(ExecutionEnvironment env) {
    DataSource<Tuple2<Long, Long>> fakeTuple = env
        .fromCollection(newArrayList(
            new Tuple2<>(Long.MAX_VALUE, Long.MIN_VALUE)));
    return fakeTuple
        .map(tuple -> new Edge<>(tuple.f0, tuple.f1, NullValue.getInstance()))
        .returns(new TypeHint<Edge<Long, NullValue>>() {})
        .filter(edge -> edge.getSource() != Long.MAX_VALUE && edge.getTarget() != Long.MIN_VALUE);
  }

  /**
   * When exact one geo property set for two tuples is valid, take it.
   */
  public static MergeGeoTuple isOnlyOneValidGeoObject(MergeGeoTuple left, MergeGeoTuple right) {
    if (isValidGeoObject(left) && !isValidGeoObject(right)) {
      return left;
    } else if (!isValidGeoObject(left) && isValidGeoObject(right)) {
      return right;
    } else {
      return null;
    }
  }

  /**
   * Geo domain only check if Triplet has valid geo lat lon attributes.
   */
  public static boolean isValidGeoObject(MergeGeoTuple triplet) {
    if (triplet.getLatitude() == null || triplet.getLongitude() == null) {
      return Boolean.FALSE;
    }
    return isValidLatitude(triplet.getLatitude()) && isValidLongitude(triplet.getLongitude());
  }

  public static boolean isValidGeoObject(Double latitude, Double longitude) {
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
   * Helper method for input data from North Carolina dataset recId.
   */
  public static long getIdFromNcId(String idString) {
    String result = Constants.EMPTY_STRING;

    for (String idPart : Splitter.on('s').split(idString)) {
      result = idPart.concat(result);
    }

    return Long.parseLong(result);
  }

  /**
   * get similarity for given strings based on metric
   */
  public static Double getSimilarityAndSimplifyForMetric(String left, String right, String metric) {
    Preconditions.checkNotNull(left);
    Preconditions.checkNotNull(right);

    if (!isSane(left) || !isSane(right)) {
//      LOG.info(left + " --- " + right);
      return null;
    }

    double similarity = getMetric(metric)
        .compare(simplify(left), simplify(right));

    return getExactDoubleResult(similarity);
  }

  /**
   * Create a string representation of a vertex for evaluation output.
   * @param vertex input vertex
   * @return resulting string
   */
  public static String toString(Vertex<Long, ObjectMap> vertex) {
    return toString(vertex, null);
  }

  @Deprecated // cikm
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
   * Legacy method to get blocking label.
   */
  public static String getMusicBlockingLabel(String label) {
    return getMusicBlockingLabel(label, 4);
  }

    /**
     * music blocking
     * Get the first x chars of string.
     *
     * Check dataset for blocked prefix values!
     */
  public static String getMusicBlockingLabel(String label, int blockingLength) {
    label = label.toLowerCase();
    String tmp = label;

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
   * @param value vertex properties
   */
  public static String createSimpleArtistTitleAlbum(ObjectMap value) {
    String artistTitleAlbum = Constants.EMPTY_STRING;
    String artist = value.getArtist();
    String label = value.getLabel();
    String album = value.getAlbum();

    if (isSane(artist)) {
      artistTitleAlbum = artist;
    }
    if (isSane(label)) {
      if (artistTitleAlbum.isEmpty()) {
        artistTitleAlbum = label;
      } else {
        artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(label);
      }
    }
    if (isSane(album)) {
      if (artistTitleAlbum.isEmpty()) {
        artistTitleAlbum = album;
      } else {
        artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(album);
      }
    }

    String tmp = artistTitleAlbum;
    artistTitleAlbum = CharMatcher.WHITESPACE.trimAndCollapseFrom(
        artistTitleAlbum.toLowerCase()
            .replaceAll("[\\p{Punct}]", " "),
        ' ');

    if (artistTitleAlbum.isEmpty()) {
//      LOG.warn("Utils: artistTitleAlbum empty: " + value.toString());
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
    return CharMatcher.WHITESPACE.trimAndCollapseFrom(
        value.toLowerCase()
            .replaceAll("[\\p{Punct}]", " "),
        ' ');
  }

  public static StringMetric getMetric(String metric) {
    switch (metric) {
      case Constants.JARO_WINKLER:
        return getJaroWinklerMetric();
      case Constants.COSINE_TRIGRAM:
        return getCosineTrigramMetric();
      default:
        throw new IllegalArgumentException("getMetric(" + metric + "): Unsupported metric: ");
    }
  }

  private static StringMetric getJaroWinklerMetric() {
    return with(new JaroWinkler())
        .build();
  }

  private static StringMetric getCosineTrigramMetric() {
    return with(new CosineSimilarity<>())
        .tokenize(Tokenizers.qGramWithPadding(3))
        .build();
    // old:
    // .simplify(Simplifiers.removeAll("[\\\\(|,].*"))
    // .simplify(Simplifiers.replaceNonWord())
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
}