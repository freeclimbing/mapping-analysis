package org.mappinganalysis.integration;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.benchmark.MusicbrainzBenchmarkTest;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
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

import java.util.Collection;
import java.util.List;

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
    new JSONDataSink(path.concat("/eighty/"), "test")
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
   * precision: 0.98905 recall: 0.88394 F1: 0.93355 <-- artistTitleAlbum (MusicSimilarityFunction)
   * precision: 0.96055 recall: 0.88726 F1: 0.92245 <-- artistTitleAlbum, year, length, language
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

//    List<Long> resultingVerticesList = Lists.newArrayList();
    Collection<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("/eighty/"), "test")
        .writeVertices(clusters);
    env.execute();

//    for (Vertex<Long, ObjectMap> representative : representatives) {
//      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
//    }
//
//    LOG.info("initial: " + resultingVerticesList.size());

    /*
      Add 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("/eighty/output/test/"), "test", true, env)
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

//    resultingVerticesList = Lists.newArrayList();
    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("/plusTen/"), "test")
        .writeVertices(clusters);
    env.execute();

//    for (Vertex<Long, ObjectMap> representative : representatives) {
//      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
//    }
//
//    LOG.info("add 10%: " + resultingVerticesList.size());

    /*
      Add a source
     */
    startingGraph = new JSONDataSource(
        path.concat("/plusTen/output/test/"), "test", true, env)
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

//    resultingVerticesList = Lists.newArrayList();
    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("/addSource/"), "test")
        .writeVertices(clusters);
    env.execute();

//    for (Vertex<Long, ObjectMap> representative : representatives) {
//      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
//    }
//
//    LOG.info("add source 5: " + resultingVerticesList.size());

    /*
      Add final 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("/addSource/output/test/"), "test", true, env)
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

//    clusters.print();
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
    new JSONDataSink(path.concat("/eighty/"), "test")
        .writeVertices(clusters);
    env.execute();

    /*
      Add 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("/eighty/output/test/"), "test", true, env)
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
        path.concat("/plusTen/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class);

    String newSource = "5";
    newVertices = baseGraph
        .getVertices()
        .filter(new SourceFilterFunction(newSource));

    config.setStep(ClusteringStep.SOURCE_ADDITION);

    IncrementalClustering plusSourceFiveClustering =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config)
            .setMatchElements(newVertices)
            .setNewSource(newSource)
            .build();

    clusters = startingGraph.run(plusSourceFiveClustering);

//    resultingVerticesList = Lists.newArrayList();
    representatives = Lists.newArrayList();
    clusters.output(new LocalCollectionOutputFormat<>(representatives));
    new JSONDataSink(path.concat("/addSource/"), "test")
        .writeVertices(clusters);
    env.execute();

//    for (Vertex<Long, ObjectMap> representative : representatives) {
//      resultingVerticesList.addAll(representative.getValue().getVerticesList());
//      LOG.info(representative.toString());
//    }
//
//    LOG.info("add source 5: " + resultingVerticesList.size());

    /*
      Add final 10% clustering
     */
    startingGraph = new JSONDataSource(
        path.concat("/addSource/output/test/"), "test", true, env)
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

//    clusters.print();
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
