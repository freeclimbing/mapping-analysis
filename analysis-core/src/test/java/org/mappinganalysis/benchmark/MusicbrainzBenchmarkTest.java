package org.mappinganalysis.benchmark;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.sun.tools.javac.util.GraphUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.mappinganalysis.NcBaseTest;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;
import org.simmetrics.StringMetric;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * songLength.equals("012-nur nooch ") // not equal?
 * songLength.contains("001-patton") // 14599 verschoben
 * songLength.equals("0e1-fizzzyblood") // 7452 verschoben
 */
public class MusicbrainzBenchmarkTest {
  private static final Logger LOG = Logger.getLogger(MusicbrainzBenchmarkTest.class);
  private static ExecutionEnvironment env;
//  public static final String TEST_TITLE = "005-Kantate, BWV 4 \"Christ lag in Todesbanden\": V. Coro Versus IV \"Es war ein wunderlicher Krieg\"";
//  public static final String TEST_TITLE = "020-Kantate, BWV 212 \"Mer hahn en neue Oberkeet\": XX. Aria (Bass) \"Dein Wachstum sei feste und lache vor Lust\",0.263117";
//  public static final String TEST_TITLE = "004-Adelante"; // 3 - 67 last match - 52 first not match
//  public static final String TEST_TITLE = "Igor Presents \"Rumors\"; Paulo Presents \"Against\"; Andreas Presents \"Hatred Aside\"; Derrick Presents \"Choke\" - Against - 4 Track Pre-Listening";
//  public static final String TEST_TITLE = "English Folk Song Suite: III. Intermezzo \"My Bonny Boy\": Andantino";
//  public static final String TEST_TITLE = "007-Chorus Finale on Schiller's 'Ode To Joy' from Symphony No. 9 in D minor, Op. 125 \"Choral\"";
  static final String TEST_TITLE = "All This Is That (Carl and the Passions: \"So Tough\")";

  /**
   * Song length test
   */
  @Test
  public void testSongLength() throws Exception {
    env = TestBase.setupLocalEnvironment();

    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices()
        .map(vertex -> {
          if (vertex.getId() == 1L) {
            assertEquals(219, vertex.getValue().getLength()); // 219
          } else if (vertex.getId() == 15184L) {
            assertEquals(220, vertex.getValue().getLength()); // 3.663
          } else if (vertex.getId() == 13138L) {
            assertEquals(147, vertex.getValue().getLength()); // 2m 27sec
          } else if (vertex.getId() == 4L) {
            assertEquals(0, vertex.getValue().getLength()); // unk.
          } else if (vertex.getId() == 15382L) {
            assertEquals(403, vertex.getValue().getLength()); // 402840
          } else if (vertex.getId() == 10291L) {
            assertEquals(222, vertex.getValue().getLength()); // 03:42
          }
          return vertex;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

//    vertices.print();
    System.out.println(vertices.count());
  }

  @Test
  public void labelSpecialTestOutput() throws Exception {
    env = TestBase.setupLocalEnvironment();

    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices();

    Map<String, Long> result = new LinkedHashMap<>();
    vertices.collect()
        .stream()
        .collect(Collectors
            .groupingBy(v -> v.getValue().getLabel(), Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> labelOccurance : result.entrySet()) {
      if (labelOccurance.getKey().matches(".*?\\s-\\s.*?")) {
//          && !labelOccurance.getKey().matches(".*? - .*?"))
//          || labelOccurance.getKey().contains(""))
        List<String> my = Lists.newArrayList();
        String[] split = labelOccurance.getKey().split(" - ");
        Collections.addAll(my, split);
        for (String s : my) {
          if (s.length() <= 3) {
            System.out.println("short: " + s);
          }
        }
//        System.out.println(labelOccurance.toString());
      }
    }
    System.out.println(vertices.count());
  }

  @Test
  public void gradoopInputTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    final String graphPath = "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000" +
//        "/user/saeedi/MusicDataset/2000k/config1_th0.7tt/";
                "/user/saeedi/MusicDataset/200k/config1_new/";
    LogicalGraph logicalGraph = Utils
        .getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.MUSIC, env);
    DataSet<Tuple2<Long, Long>> evalResultList = graph.getVertices()
        .map(vertex -> new Tuple2<>(vertex.getId(), (long) vertex.getValue().get("clsId")))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.BLOCK_SPLIT);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.5);
    config.setBlockingLength(4);
    config.setMinResultSimilarity(0.8);
    config.setExistingSourcesCount(5);

    DataSet<Vertex<Long, ObjectMap>> clusters = graph
        .run(new DefaultPreprocessing(config))
        .run(new TypeGroupBy(env))
        .run(new SimSort(config))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(config.getDataDomain()));

    List<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    env.execute();

    DataSet<Vertex<Long, ObjectMap>> resultingClusters = env
        .fromCollection(representatives)
        .runOperation(new MergeExecution(config));

    representatives = Lists.newArrayList();
    resultingClusters.output(new LocalCollectionOutputFormat<>(representatives));
    env.execute();

    QualityUtils.printNewMusicQuality(env.fromCollection(representatives),
        config,
        graphPath,
        evalResultList,
        "2000k",
        "local");
  }

  /**
   * other modules can not be used everywhere
   */
  @Test
  public void testSim() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> inputVertices =
        new CSVDataSource(path, vertexFileName, env)
        .getVertices();

    DataSet<Edge<Long, NullValue>> inputEdges = inputVertices
        .runOperation(new EdgeComputationOnVerticesForKeySelector(
            new CcIdKeySelector(),
            EdgeComputationStrategy.SIMPLE));

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    Graph<Long, ObjectMap, ObjectMap> graph = Graph
        .fromDataSet(inputVertices, inputEdges, env)
        .run(new DefaultPreprocessing(config));

//    graph.getVertices().print();
    graph.getEdges().print();

//    String[] split = TEST_TITLE.split("\\\"");
//    Matcher quoted = Pattern.compile("\"(.*?)\"").matcher(TEST_TITLE);
//
//    Map<String, Double> result = new LinkedHashMap<>();
//
//    List<Tuple2<String, Double>> collect = vertices.map(new MaxMapFunction())
//        .collect()
//        .stream()
//        .sorted((left, right) -> left.f1.compareTo(right.f1))
//        .collect(Collectors.toList());

//    for (Map.Entry<String, Double> stringDoubleEntry : result.entrySet()) {
//      System.out.println(stringDoubleEntry);
//    }

//    for (Tuple2<String, Double> tuple : collect) {
//      if (tuple.f1 > 0.4)
//      System.out.println(tuple);
//    }
  }

  @Test
  public void lengthTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices();

    Map<String, Long> result = new LinkedHashMap<>();

    vertices
        .collect()
        .stream()
        .collect(Collectors
            .groupingBy(v -> String.valueOf(v.getValue().getLength()), Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> entry : result.entrySet()) {
      if (entry.getKey().equals("30")) { // 30 seconds most often
        assertEquals(129, entry.getValue().intValue());
      }
//      System.out.println(entry.toString());
    }
  }

  @Test
  public void languageTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices();
    Map<String, Long> result = new LinkedHashMap<>();

    vertices
        .collect()
        .stream()
        .collect(Collectors
            .groupingBy(v -> v.getValue().get(Constants.LANGUAGE).toString(),
                Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> entry : result.entrySet()) {
//      System.out.println(entry.toString());
      if (entry.getKey().equals(Constants.EN)) { // en most often
        assertEquals(10462, entry.getValue().intValue());
      }
    }
  }

  @Test
  public void basicBlockingLabelTest() throws Exception {
    String test = "003-Symphony 1 in C minor,  some  op. 11: III. Menuetto & Trio";
    String musicBlockingLabel = Utils.getMusicBlockingLabel(test, 4);

    assertTrue(musicBlockingLabel.equals("hony"));
  }

  /**
   * Show blocking label distribution
   */
  @Test
  public void findPaperExampleTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices();

    DataSet<Tuple2<String, Double>> johnResults = vertices
        .map(vertex -> Utils.createSimpleArtistTitleAlbum(vertex.getValue())).returns(new TypeHint<String>() {
        }).filter(vertex -> vertex.startsWith("john")).map(value -> {
          String compare = "johnny cash ring of fire san quentin other hits";
          return new Tuple2<>(value, Utils.getSimilarityAndSimplifyForMetric(value, compare, Constants.COSINE_TRIGRAM));
        }).returns(new TypeHint<Tuple2<String, Double>>() {
        });

    List<Tuple2<String, Double>> collect = johnResults.collect();
    collect
        .stream()
        .sorted(Comparator.comparing(x->x.f1))
    .forEach(System.out::println);
  }

  /**
   * Show blocking label distribution
   */
  @Test
  public void blockingLabelTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    DataSet<Vertex<Long, ObjectMap>> vertices
        = new CSVDataSource(path, vertexFileName, env)
        .getVertices();
    Map<String, Long> result = new LinkedHashMap<>();

    vertices
        .collect()
        .stream()
        .collect(Collectors
            .groupingBy(v ->
                    Utils.getMusicBlockingLabel(
                Utils.createSimpleArtistTitleAlbum(v.getValue()),
                        5),
                Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> resultEntry : result.entrySet()) {
      System.out.println(resultEntry.toString());
//      if (resultEntry.getValue() == 58L) {
//        assertTrue(resultEntry.getKey().equals("rock"));
//      }
//      assertTrue(58L >= resultEntry.getValue());
    }
  }
  /**
   * special for big input file, not a real test
   *
   * 5min - change log destination in properties file
   */
//  @Test
//  public void extendedBlockingLabelTest() throws Exception {
//    env = setupLocalEnvironment();
//
//    String path = MusicbrainzBenchmarkTest.class
//        .getResource("/data/musicbrainz/")
//        .getFile();
////    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
//    final String[] vertexFileNames = new String[]{"xaa", "xab", "xac", "xad", "xae", "xaf", "xag", "xah", "xai", "xaj"};
//
//    for (String vertexFileName : vertexFileNames) {
//
//      DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
//          .getVertices();
//
//      Map<String, Long> result = new LinkedHashMap<>();
//
//      vertices
//          .collect()
//          .stream()
//          .collect(Collectors
//              .groupingBy(v -> {
//                    String artistTitleAlbum = Constants.EMPTY_STRING;
//                    String artist = v.getValue().getArtist();
//                    if (!artist.equals(Constants.CSV_NO_VALUE)) {
//                      artistTitleAlbum = artist;
//                    }
//                    String label = v.getValue().getLabel();
//                    if (!label.equals(Constants.CSV_NO_VALUE)) {
//                      if (artistTitleAlbum.equals(Constants.EMPTY_STRING)) {
//                        artistTitleAlbum = label;
//                      } else {
//                        artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(label);
//                      }
//                    }
//                    String album = v.getValue().getAlbum();
//                    if (!album.equals(Constants.CSV_NO_VALUE)) {
//                      if (artistTitleAlbum.equals(Constants.EMPTY_STRING)) {
//                        artistTitleAlbum = album;
//                      } else {
//                        artistTitleAlbum = artistTitleAlbum.concat(Constants.DEVIDER).concat(album);
//                      }
//
//                    }
//                    return Utils.getMusicBlockingLabel(artistTitleAlbum);
//                  }
//                  , Collectors.counting()))
////        .groupingBy(v -> v.getValue().get(Constants.LABEL).toString(), Collectors.counting()))
//          .entrySet()
//          .stream()
//          .sorted(Map.Entry.<String, Long>comparingByValue())
//          .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));
//
//      for (Map.Entry<String, Long> stringLongEntry : result.entrySet()) {
//        LOG.info(stringLongEntry.toString());
////      System.out.println(stringLongEntry.toString());
//      }
//    }
//  }

  // TODO if 107=1 is found, what is the vertex?
  @Test
  public void yearTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    Map<String, Long> result = new LinkedHashMap<>();
    new CSVDataSource(path, vertexFileName, env)
        .getVertices()
        .collect()
        .stream()
        .collect(Collectors
            .groupingBy(v -> v.getValue().getYear().toString(), Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> resultLine : result.entrySet()) {
      Integer year = Ints.tryParse(resultLine.getKey());
//      System.out.println(year);
//      if (year == null) {
//        System.out.println(resultLine.getKey());
//      }
//      assertTrue((year <= 2006 && year >= 107) || year == 0);
      if (year == 2006) {
        assertEquals(725L, resultLine.getValue().longValue());
      }
//      System.out.println(resultLine.toString());
    }
  }

  @Test
  public void testTest() throws Exception {

    String a = "ab99o9sssss1956lll";
    String b = "aaaaaaa3d4d4d4";
    Pattern fourDigits = Pattern.compile(".*(\\d{4}).*");
    Matcher matcher = fourDigits.matcher(a);
    if (matcher.find()) {
      System.out.println(matcher.groupCount());
      for (int i=0; i<= matcher.groupCount(); i++) {
        System.out.println(matcher.group(i));
      }
      System.out.println(matcher.group(1));
    }


  }

  /**
   * additional split for strings containing - as separator?
   */
  private static class MaxMapFunction
      implements MapFunction<Vertex<Long,ObjectMap>, Tuple2<String, Double>> {

    @Override
    public Tuple2<String, Double> map(Vertex<Long, ObjectMap> vertex) throws Exception {
      String label = vertex.getValue().getLabel();
      StringMetric metric = Utils.getMetric(Constants.COSINE_TRIGRAM);

      Pattern rightPattern = Pattern.compile("\"(.*?)\""); // only use for hard coded test strings

      Matcher rightSide = rightPattern.matcher(TEST_TITLE);
      Set<String> rightSet = Sets.newHashSet();
      while (rightSide.find()) {
        rightSet.add(rightSide.group(1));
      }

      Pattern leftPattern = Pattern.compile("\\\\\"(.*?)\\\\\"");
      Matcher leftSide = leftPattern.matcher(label);
      HashMap<String, Double> tmpMap = Maps.newHashMap();
      while (!rightSet.isEmpty() && leftSide.find()) {
        Double similarity = null;
        String left = Utils.simplify(leftSide.group(1));
        for (String right : rightSet) {
          right = Utils.simplify(right);
          System.out.println(left + " ##### " + right);
          double tmp = metric.compare(left, right);
          BigDecimal tmpResult = new BigDecimal(tmp);
          tmp = tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
          System.out.println("while loop sim: " + tmp);
          if (similarity == null || tmp > similarity) {
            similarity = tmp;
          }
        }

        tmpMap.put(left, similarity);
      }

      // TODO check min sim compatibility
      double similarity = metric.compare(label.trim(), TEST_TITLE.trim());
      BigDecimal tmpResult = new BigDecimal(similarity);
      similarity = tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();

      if (!tmpMap.isEmpty() && Collections.max(tmpMap.values()) > similarity) {
        return new Tuple2<>(label, Collections.max(tmpMap.values()));
      } else {
        return new Tuple2<>(label, similarity);
      }
    }
  }
}