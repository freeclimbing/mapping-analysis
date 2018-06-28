package org.mappinganalysis.integration;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.benchmark.MusicbrainzBenchmarkTest;
import org.mappinganalysis.model.functions.incremental.MatchingStrategy;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.io.impl.json.JSONToEdgeFormatter;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.SubGraphFromIds;
import org.mappinganalysis.model.functions.SubGraphVertexExtraction;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.reverseOrder;
import static org.junit.Assert.assertEquals;

public class IncrementalMusicClusteringTest {
  private static final Logger LOG = Logger.getLogger(IncrementalMusicClusteringTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  /**
   * precision: 0.99215 recall: 0.88689 F1: 0.93657
   */
  @Test
  public void initialMusicTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    Graph<Long, ObjectMap, NullValue> baseGraph =
        new CSVDataSource(path, vertexFileName, env)
            .getGraph();

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(ClusteringStep.INITIAL_CLUSTERING);
    config.setSimSortSimilarity(0.7);
    config.setMinResultSimilarity(0.6);

    IncrementalClustering initialClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .build();

    /*
      start clustering
     */
    DataSet<Vertex<Long, ObjectMap>> clusters = baseGraph.run(initialClustering);

    List<Long> resultingVerticesList = Lists.newArrayList();
    Collection<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("eighty/"), "test")
        .writeVertices(clusters);
    env.execute();

//    for (Vertex<Long, ObjectMap> representative : representatives) {
//      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
//    }

    QualityUtils.printMusicQuality(clusters, config);
  }

  /**
   * default setting, initial clustering 80%, add 10%, add a source, add final 10%
   *
   * including source addition via hungarian algorithm
   *
   * precision: 0.98905 recall: 0.88394 F1: 0.93355 <-- artistTitleAlbum (MusicSimilarityFunction)
   * precision: 0.96055 recall: 0.88726 F1: 0.92245 <-- artistTitleAlbum, year, length, language
   *
   * ############### dataset: {isIncremental=true, step=VERTEX_ADDITION,
   * blockingStrategy=STANDARD_BLOCKING, incrementalStrategy=MULTI, metric=ct,
   * dataDomain=MUSIC, simsortThreshold=0.7, mode=music, env=Local Environment
   * (parallelism = 8) : 37b1d3610aa5c788e626877bab5c8344,
TP+FN: 16250
TP+FP: 14520
TP: 14364
precision: 0.9892561983471074 recall: 0.8839384615384616 F1: 0.9336366590835229
######################################################
   */
  @Test
  public void musicIncrementalTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    Graph<Long, ObjectMap, NullValue> baseGraph =
        new CSVDataSource(path, vertexFileName, env)
            .getGraph();

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(ClusteringStep.INITIAL_CLUSTERING);
    config.setSubGraphVerticesPath(path.concat("split/initialEighty.txt"));
    config.setSimSortSimilarity(0.7);

    IncrementalClustering initialClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .build();

    Graph<Long, ObjectMap, NullValue> startingGraph = baseGraph
        .run(new SubGraphFromIds(config));

    /*
      start clustering
     */
    DataSet<Vertex<Long, ObjectMap>> clusters = startingGraph
        .run(initialClustering);

    Collection<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("eighty/"), "test")
        .writeVertices(clusters);
    env.execute();

    /*
      Add 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("eighty/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class);
    config.setStep(ClusteringStep.VERTEX_ADDITION);
    config.setSubGraphVerticesPath(path.concat("split/addTen.txt"));

    DataSet<Vertex<Long, ObjectMap>> newVertices = baseGraph
        .run(new SubGraphVertexExtraction(config));

    IncrementalClustering plusTenClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .build();

    clusters = startingGraph.run(plusTenClustering);

    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("plusTen/"), "test")
        .writeVertices(clusters);
    env.execute();

    /*
      Add a source
     */
    startingGraph = new JSONDataSource(
        path.concat("plusTen/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class);

    newVertices = baseGraph
        .getVertices()
        .filter(new SourceFilterFunction("5"));

    IncrementalClustering plusSourceFiveClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .build();

    clusters = startingGraph.run(plusSourceFiveClustering);

    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("addSource/"), "test")
        .writeVertices(clusters);
    env.execute();

    /*
      Add final 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("addSource/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class);
    config.setStep(ClusteringStep.VERTEX_ADDITION);
    config.setSubGraphVerticesPath(path.concat("split/lastTen.txt"));

    newVertices = baseGraph
        .run(new SubGraphVertexExtraction(config));

    IncrementalClustering finalClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .build();

    clusters = startingGraph.run(finalClustering);

//    resultingVerticesList = Lists.newArrayList();
    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("/finalClustering/"), "test")
        .writeVertices(clusters);
    env.execute();

//    for (Vertex<Long, ObjectMap> representative : representatives) {
//      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
//    }
//
//    LOG.info("final 10%: " + resultingVerticesList.size());

//    assertEquals(12439, resultingVerticesList.size());
      QualityUtils.printMusicQuality(clusters, config);
  }

  @Test
  public void musicIncHungarianTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    Graph<Long, ObjectMap, NullValue> baseGraph =
        new CSVDataSource(path, vertexFileName, env)
            .getGraph();

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(ClusteringStep.INITIAL_CLUSTERING);
    config.setSubGraphVerticesPath(path.concat("split/initialEighty.txt"));
    config.setSimSortSimilarity(0.7);

    IncrementalClustering initialClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .build();

    Graph<Long, ObjectMap, NullValue> startingGraph = baseGraph
        .run(new SubGraphFromIds(config));

    /*
      start clustering
     */
    DataSet<Vertex<Long, ObjectMap>> clusters = startingGraph
        .run(initialClustering);

    Collection<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("eighty/"), "test")
        .writeVertices(clusters);
    env.execute();

    /*
      Add 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("eighty/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class);
    config.setStep(ClusteringStep.VERTEX_ADDITION);
    config.setSubGraphVerticesPath(path.concat("split/addTen.txt"));

    DataSet<Vertex<Long, ObjectMap>> newVertices = baseGraph
        .run(new SubGraphVertexExtraction(config));

    IncrementalClustering plusTenClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .build();

    clusters = startingGraph.run(plusTenClustering);

    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("/plusTen/"), "test")
        .writeVertices(clusters);
    env.execute();

    /*
      Add a source
     */
    startingGraph = new JSONDataSource(
        path.concat("plusTen/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class);

    String newSource = "5";
    newVertices = baseGraph
        .getVertices()
        .filter(new SourceFilterFunction(newSource));

    config.setStep(ClusteringStep.SOURCE_ADDITION);
    config.setMatchStrategy(MatchingStrategy.MAX_BOTH);

    IncrementalClustering plusSourceFiveClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .setNewSource(newSource)
            .build();

    clusters = startingGraph.run(plusSourceFiveClustering);

    List<Long> resultingVerticesList = Lists.newArrayList();
    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("addSource/"), "test")
        .writeVertices(clusters);
    env.execute();

    for (Vertex<Long, ObjectMap> representative : representatives) {
      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
    }

    LOG.info("add source 5: " + resultingVerticesList.size());

    /*
      Add final 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("addSource/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class);
    config.setStep(ClusteringStep.VERTEX_ADDITION);
    config.setSubGraphVerticesPath(path.concat("split/lastTen.txt"));

    newVertices = baseGraph
        .run(new SubGraphVertexExtraction(config));

    IncrementalClustering finalClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .build();

    clusters = startingGraph.run(finalClustering);

    resultingVerticesList = Lists.newArrayList();
    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("/finalClustering/"), "test")
        .writeVertices(clusters);
    env.execute();

    for (Vertex<Long, ObjectMap> representative : representatives) {
      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
    }

    LOG.info("final 10%: " + resultingVerticesList.size());

//    assertEquals(12439, resultingVerticesList.size());
    QualityUtils.printMusicQuality(env.fromCollection(representatives), config);

//    clusters.print();
  }

  /**
   * Heap space exception when all permutations are run after each other, need to restart multiple times
   * for all results
   */
  @Test
  public void musicSourceBySourceTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/").getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    Graph<Long, ObjectMap, NullValue> baseGraph =
        new CSVDataSource(path, vertexFileName, env)
            .getGraph();

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(ClusteringStep.SOURCE_ADDITION);
    config.setSimSortSimilarity(0.7);

    HashMap<String, BigDecimal> resultMap = Maps.newHashMap();
    List<String> sourcesList = Constants.MUSIC_SOURCES;
    List<Vertex<Long, ObjectMap>> representatives = null;
    Collection<List<String>> permutedLists = null;

    DataSet<Vertex<Long, ObjectMap>> clusters;
    representatives = null;

    for (int i = 4; i <= 4; i++) {
      LOG.info("###### Working on source: " + (i+1) + "######");

      List<String> tempList = Lists.newArrayList(sourcesList);
      final Graph<Long, ObjectMap, NullValue> workingGraph = baseGraph
          .filterOnVertices(new SourceFilterFunction(sourcesList.get(i)));
      tempList.remove(sourcesList.get(i));
      LOG.info("other sources: " + String.join(", ", tempList));
      permutedLists = Collections2.orderedPermutations(tempList);
      int permuteRun = 0;

      for (List<String> list : permutedLists) {
        permuteRun++;
        LOG.info("run: " + permuteRun);

        Graph<Long, ObjectMap, NullValue> permutedInputGraph;
        boolean isSecond = true;
        for (String source : list) {
          LOG.info("Working on source: " + source);
          if (isSecond) { // second, working graph should not be read from file
            permutedInputGraph = workingGraph;
            isSecond = false;
          } else {
            permutedInputGraph = new JSONDataSource(
                path.concat("output/test/"), "test", true, env)
                .getGraph(ObjectMap.class, NullValue.class);
          }
          DataSet<Vertex<Long, ObjectMap>> newVertices = baseGraph
              .getVertices()
              .filter(new SourceFilterFunction(source));

          IncrementalClustering clustering = new IncrementalClustering
              .IncrementalClusteringBuilder(config)
              .setMatchElements(newVertices)
              .setNewSource(source)
              .build();

          clusters = permutedInputGraph.run(clustering);

          representatives = Lists.newArrayList();
          clusters.output(new LocalCollectionOutputFormat<>(representatives));
          new JSONDataSink(path, "test")
              .writeVertices(clusters);
          JobExecutionResult execute = env.execute();
          LOG.info("Single Flink Job time (s): " + execute.getNetRuntime(TimeUnit.SECONDS));
          TimeUnit.SECONDS.sleep(1);
        }

        assert representatives != null;
        HashMap<String, BigDecimal> singleResult = QualityUtils
            .printMusicQuality(env.fromCollection(representatives), config);

        String singleString = String.join(", ", list);
        singleString = singleString.concat("### pr: " + singleResult.get("precision")
            .setScale(4, BigDecimal.ROUND_HALF_UP))
            .concat(" re: " + singleResult.get("recall")
                .setScale(4, BigDecimal.ROUND_HALF_UP));
        resultMap.put( singleString, singleResult.get("f1")
            .setScale(4, BigDecimal.ROUND_HALF_UP));
        TimeUnit.SECONDS.sleep(1);
      }
    }

    /*
      actual tests
     */
    List<Long> resultingVerticesList = Lists.newArrayList();
    assert representatives != null;
    for (Vertex<Long, ObjectMap> representative : representatives) {
      resultingVerticesList.addAll(representative.getValue().getVerticesList());
    }
    HashSet<Long> uniqueVerticesSet = Sets.newHashSet(resultingVerticesList);
    assertEquals(resultingVerticesList.size(), uniqueVerticesSet.size());
    assertEquals(19375, resultingVerticesList.size());

    resultMap.entrySet()
        .stream()
        .sorted(reverseOrder(Map.Entry.comparingByValue()))
        .forEach(System.out::println);
  }

  /**
   * Test, if source addition can be executed multiple times without
   * env.execute() in between.
   */
  @Test
  public void allInOneTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/").getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    Graph<Long, ObjectMap, NullValue> baseGraph
        = new CSVDataSource(path, vertexFileName, env)
        .getGraph();

    IncrementalConfig config = new IncrementalConfig(DataDomain.MUSIC, env);
    config.setBlockingStrategy(BlockingStrategy.STANDARD_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.MULTI);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setStep(ClusteringStep.SOURCE_ADDITION);
    config.setSimSortSimilarity(0.7);
    config.setMatchStrategy(MatchingStrategy.MAX_BOTH);

    List<String> musicSources = Constants.MUSIC_SOURCES;
    Graph<Long, ObjectMap, NullValue> workingGraph = null;
    DataSet<Vertex<Long, ObjectMap>> clusters = null;

    boolean isFirst = true;
    for (String musicSource : musicSources) {
      if (isFirst) {
        workingGraph = baseGraph.filterOnVertices(new SourceFilterFunction(musicSource));
        isFirst = false;
      } else {
        DataSet<Vertex<Long, ObjectMap>> newVertices = baseGraph
            .getVertices()
            .filter(new SourceFilterFunction(musicSource));

        IncrementalClustering clustering = new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .setNewSource(musicSource)
            .build();

        clusters = workingGraph.run(clustering);

        DataSet<Edge<Long, NullValue>> edges = env.fromCollection(
            Lists.newArrayList(""))
            .map(new JSONToEdgeFormatter<>(NullValue.class));

        workingGraph = Graph.fromDataSet(clusters, edges, env);
      }
    }

    assert clusters != null;

    List<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));

    List<Long> resultingVerticesList = Lists.newArrayList();
    for (Vertex<Long, ObjectMap> representative : representatives) {
      resultingVerticesList.addAll(representative.getValue().getVerticesList());
    }
    HashSet<Long> uniqueVerticesSet = Sets.newHashSet(resultingVerticesList);
    assertEquals(resultingVerticesList.size(), uniqueVerticesSet.size());
    env.execute();

    QualityUtils.printMusicQuality(env.fromCollection(representatives), config);
//    clusters.print();
  }

  @Test
  public void listTest() throws Exception {
    List<String> sourcesList = Lists.newArrayList("1", "2", "3", "4", "5");
    for (String s : sourcesList) {
      LOG.info(s);
    }

    Collection<List<String>> lists = Collections2.orderedPermutations(sourcesList);

    for (List<String> list : lists) {
      LOG.info(String.join(",", list));
    }

    Map<String, BigDecimal> map = Maps.newHashMap();

    map.put("1", new BigDecimal(0.9));
    map.put("2", new BigDecimal(0.8));
    map.put("3", new BigDecimal(0.95));

    List<Map.Entry<String, BigDecimal>> sorted_map =
        map.entrySet()
            .stream()
            .sorted(reverseOrder(Map.Entry.comparingByValue()))
            .collect(Collectors.toList());

    for (Map.Entry<String, BigDecimal> stringBigDecimalEntry : sorted_map) {
      LOG.info(stringBigDecimalEntry.toString());
    }
  }
//  /**
//   * helper function
//   */
//  @Test
//  public void splitCreation() throws Exception {
//    final String path = MusicbrainzBenchmarkTest.class
//        .getResource("/data/musicbrainz/")
//        .getFile();
//    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
//    Graph<Long, ObjectMap, NullValue> baseGraph =
//        new CSVDataSource(path, vertexFileName, env)
//            .getGraph();
//
//    List<Vertex<Long, ObjectMap>> removeOneSourceVertices = baseGraph
//        .getVertices()
//        .filter(new SourceFilterFunction("5"))
//        .collect();
//
//    Graph<Long, ObjectMap, NullValue> input = baseGraph.removeVertices(removeOneSourceVertices);
//
//    List<Vertex<Long, ObjectMap>> vertices = input.getVertices().collect();
//    Collections.shuffle(vertices);
//
//    String first80gn = "80: ";
//    String add10gn = "10-add: ";
//    String final10gn = "10-final: ";
//    int counter = 0;
//    int all = 0;
//    int first = 0;
//    int add = 0;
//    int rest = 0;
//    for (Vertex<Long, ObjectMap> vertex : vertices) {
//      if (counter <= 7) {
//        first80gn = first80gn.concat(vertex.getId().toString())
//            .concat(" ");
//        ++first;
//      } else if (counter == 8) {
//        add10gn = add10gn.concat(vertex.getId().toString())
//            .concat(" ");
//        ++add;
//      } else if (counter == 9) {
//        final10gn = final10gn.concat(vertex.getId().toString())
//            .concat(" ");
//        ++rest;
//      }
//
//      ++counter;
//      ++all;
//      if (counter == 10) {
//        counter = 0;
//      }
//    }
//
//    LOG.info(first80gn);
//    LOG.info(add10gn);
//    LOG.info(final10gn);
//    LOG.info(first + " " + add + " " + rest + " " + all);
//  }
}