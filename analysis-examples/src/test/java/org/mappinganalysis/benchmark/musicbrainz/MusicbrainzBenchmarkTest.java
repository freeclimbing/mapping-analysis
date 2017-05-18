package org.mappinganalysis.benchmark.musicbrainz;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;
import org.simmetrics.StringMetric;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Created by markus on 3/31/17.
 *
 * songLength.equals("012-nur nooch ") // not equal?
 * songLength.contains("001-patton") // 14599 verschoben
 * songLength.equals("0e1-fizzzyblood") // 7452 verschoben
 */
public class MusicbrainzBenchmarkTest {
  private static ExecutionEnvironment env;
//  public static final String TEST_TITLE = "005-Kantate, BWV 4 \"Christ lag in Todesbanden\": V. Coro Versus IV \"Es war ein wunderlicher Krieg\"";
//  public static final String TEST_TITLE = "020-Kantate, BWV 212 \"Mer hahn en neue Oberkeet\": XX. Aria (Bass) \"Dein Wachstum sei feste und lache vor Lust\",0.263117";
//  public static final String TEST_TITLE = "004-Adelante"; // 3 - 67 last match - 52 first not match
//  public static final String TEST_TITLE = "Igor Presents \"Rumors\"; Paulo Presents \"Against\"; Andreas Presents \"Hatred Aside\"; Derrick Presents \"Choke\" - Against - 4 Track Pre-Listening";
//  public static final String TEST_TITLE = "English Folk Song Suite: III. Intermezzo \"My Bonny Boy\": Andantino";
//  public static final String TEST_TITLE = "007-Chorus Finale on Schiller's 'Ode To Joy' from Symphony No. 9 in D minor, Op. 125 \"Choral\"";
  public static final String TEST_TITLE = "All This Is That (Carl and the Passions: \"So Tough\")";

  /**
   * Song length test
   */
  @Test
  public void testSongLength() throws Exception {
    env = setupLocalEnvironment();

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
    env = setupLocalEnvironment();

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
        .sorted(Map.Entry.<String, Long>comparingByValue())
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

  /**
   * other modules can not be used everywhere
   * @throws Exception
   */
  @Test
  public void testSim() throws Exception {
    env = setupLocalEnvironment();

    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
//    final String vertexFileName = "musicbrainz-20000000-A01.csv.dapo";
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> inputVertivces = new CSVDataSource(path, vertexFileName, env)
        .getVertices();

    DataSet<Edge<Long, NullValue>> inputEdges = inputVertivces
        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector(), false));

    Graph<Long, ObjectMap, ObjectMap> graph = Graph.fromDataSet(inputVertivces, inputEdges, env)
//        .run(new BasicEdgeSimilarityComputation(Constants.MUSIC, env)); // working similarity run
        .run(new DefaultPreprocessing(DataDomain.MUSIC, env));

//    graph.getVertices().print();
//    graph.getEdges().print();

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
    env = setupLocalEnvironment();

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
        .sorted(Map.Entry.<String, Long>comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> entry : result.entrySet()) {
      System.out.println(entry.toString());
    }
  }

  @Test
  public void languageTest() throws Exception {
    env = setupLocalEnvironment();

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
            .groupingBy(v -> v.getValue().get(Constants.LANGUAGE).toString(), Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.<String, Long>comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> stringLongEntry : result.entrySet()) {
      System.out.println(stringLongEntry.toString());
    }
  }

  /**
   * Show blocking label distribution
   * @throws Exception
   */
  @Test
  public void blockingLabelTest() throws Exception {
    env = setupLocalEnvironment();

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
            .groupingBy(v -> Utils.getMusicBlockingLabel(v.getValue().get(Constants.LABEL).toString()),
                Collectors.counting()))
//        .groupingBy(v -> v.getValue().get(Constants.LABEL).toString(), Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.<String, Long>comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> stringLongEntry : result.entrySet()) {
      System.out.println(stringLongEntry.toString());
    }
  }

  @Test
  public void yearTest() throws Exception {
    env = setupLocalEnvironment();

    String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices()
//        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {
//          @Override
//          public boolean filter(Vertex<Long, ObjectMap> value) throws Exception {
//            return value.getValue().containsKey("year")
//                && value.getValue().get("year") != null;
////                && value.getValue().get("year") == null;
//          }
//        })
        ;

    Map<String, Long> result = new LinkedHashMap<>();
    vertices.collect()
        .stream()
        .collect(Collectors
            .groupingBy(v -> v.getValue().getYear().toString(), Collectors.counting()))
        .entrySet()
        .stream()
        .sorted(Map.Entry.<String, Long>comparingByValue())
        .forEachOrdered(x -> result.put(x.getKey(), x.getValue()));

    for (Map.Entry<String, Long> stringLongEntry : result.entrySet()) {
      System.out.println(stringLongEntry.toString());
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

  public static ExecutionEnvironment setupLocalEnvironment() {
    Configuration conf = new Configuration();
    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 16384);
    env = new LocalEnvironment(conf);
    env.setParallelism(Runtime.getRuntime().availableProcessors());
    env.getConfig().disableSysoutLogging();

    return env;
  }

  /**
   * additional split for strings containing - as separator?
   */
  private static class MaxMapFunction
      implements MapFunction<Vertex<Long,ObjectMap>, Tuple2<String, Double>> {

    @Override
    public Tuple2<String, Double> map(Vertex<Long, ObjectMap> vertex) throws Exception {
      String label = vertex.getValue().getLabel();
      StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();

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
        String left = leftSide.group(1);
        for (String right : rightSet) {
          System.out.println(left + " ##### " + right);
          double tmp = metric.compare(left.trim(), right.trim());
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