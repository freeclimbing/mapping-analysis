package org.mappinganalysis;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.mappinganalysis.corruption.EdgeCreateCorruptionFunction;
import org.mappinganalysis.corruption.EdgeRemoveCorruptionFunction;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.graph.utils.AllEdgesCreateGroupReducer;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationOnVerticesForKeySelector;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.*;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.QualityEdgeCreator;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MusicBrainzTest {
  private static final Logger LOG = Logger.getLogger(MusicBrainzTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void testEdgeRemoveCorruption() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicBrainzTest.class
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

    assertEquals(9375, inputEdges.count());
//    System.out.println(inputEdges.count());

    DataSet<Edge<Long, NullValue>> edges = inputEdges
        .mapPartition(new EdgeRemoveCorruptionFunction(10));

//    System.out.println(edges.count());
    assertEquals(8526, edges.count());
  }

  @Test
  public void testEdgeAddCorruption() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicBrainzTest.class
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

    assertEquals(9375, inputEdges.count());
//    System.out.println(inputEdges.count());

    DataSet<Edge<Long, NullValue>> newEdges = inputVertices
        .map(new MapFunction<Vertex<Long, ObjectMap>, Long>() {
          @Override
          public Long map(Vertex<Long, ObjectMap> value) throws Exception {
            return value.getId();
          }
        })
        .mapPartition(new EdgeCreateCorruptionFunction(10));
    assertEquals(1758, newEdges.count());
//    System.out.println(newEdges.count());

    DataSet<Edge<Long, NullValue>> unionEdges = inputEdges
        .union(newEdges)
        .distinct();
    assertEquals(11133, unionEdges.count());
//    System.out.println(unionEdges.count());
  }

  /**
   * Musicbrainz test for csimq paper with input from Alieh.
   */
  @Test
  public void csimqTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String graphPath =
        "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/nentwig/musicbrainz/csimq/";
    List<String> sourceList = Lists.newArrayList(
//        "1/"
//        ,
        "2/", "3/"
    );
    for (String dataset : sourceList) {
      String pmPath = graphPath.concat(dataset);
      LogicalGraph logicalGraph = Utils
          .getGradoopGraph(pmPath, env);
      Graph<Long, ObjectMap, NullValue> inputGraph = Utils
          .getInputGraph(logicalGraph, Constants.MUSIC, env);

      LOG.info("inEdges: " + inputGraph.getEdgeIds().count());
      Graph<Long, ObjectMap, ObjectMap> graph = inputGraph
          .run(new DefaultPreprocessing(DataDomain.MUSIC, env));


//      for (int simFor = 20; simFor <= 30; simFor += 5) {
//        double simThreshold = (double) simFor / 100;
      double simThreshold = 0.4;
    /*
       representative creation
     */
        DataSet<Vertex<Long, ObjectMap>> representatives = graph
            .run(new TypeGroupBy(env))
            .run(new SimSort(DataDomain.MUSIC, Constants.COSINE_TRIGRAM, simThreshold, env))
            .getVertices()
            .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.MUSIC));

      /*
        tmp solution
       */
        String outPath = "/home/markus/repos/mapping-analysis/analysis-core/target/test-classes/data/musicbrainz/csimq/";
        String reprOut = outPath.concat("/output/");
        new JSONDataSink(reprOut, "repr")
            .writeVertices(representatives);
//        printQuality(dataset, 0.0, simThreshold, representatives, Constants.EMPTY_STRING, 5);
//      } // sim for
      env.execute();

    // merge
      DataSet<Vertex<Long, ObjectMap>> diskRepresentatives =
          new org.mappinganalysis.io.impl.json.JSONDataSource(
              reprOut.concat("output/repr/"), true, env)
              .getVertices();

      for (int mergeFor = 40; mergeFor <= 95; mergeFor += 5) {
        double mergeThreshold = (double) mergeFor / 100;

        DataSet<Vertex<Long, ObjectMap>> merged = diskRepresentatives
            .runOperation(new MergeInitialization(DataDomain.MUSIC))
            .runOperation(new MergeExecution(
                DataDomain.MUSIC,
                Constants.COSINE_TRIGRAM,
                mergeThreshold,
                5,
                env));

        printQuality(dataset, mergeThreshold, 0.4, merged, Constants.EMPTY_STRING, 5);
      }
    }
  }

  private void printQuality(
      String dataset,
      double mergeThreshold,
      double simSortThreshold,
      DataSet<Vertex<Long, ObjectMap>> merged,
      String pmPath,
      int sourcesCount) throws Exception {
    if (mergeThreshold == 0.0) {
      dataset = dataset.concat("REPR");
    }
    DataSet<Tuple2<Long, Long>> clusterEdges = merged
        .flatMap(new QualityEdgeCreator());

    String path = "/data/musicbrainz/input/";
    DataSet<Tuple2<Long, Long>> goldLinks;

    if (pmPath.equals(Constants.EMPTY_STRING)) {
      pmPath = NorthCarolinaVoterBaseTest.class
          .getResource(path).getFile();

      DataSet<Tuple2<String, String>> perfectMapping = env
          .readCsvFile(pmPath.concat("musicbrainz-20000-A01.csv.dapo"))
          .ignoreFirstLine()
          .includeFields(true, true, false, false, false, false, false, false, false, false, false, false)
          .types(String.class, String.class);

      goldLinks = perfectMapping
          .map(tuple -> new Vertex<>(Long.parseLong(tuple.f0), Long.parseLong(tuple.f1)))
          .returns(new TypeHint<Vertex<Long, Long>>() {})
          .groupBy(1)
          .reduceGroup(new AllEdgesCreateGroupReducer<>())
          .map(edge -> new Tuple2<>(edge.getSource(), edge.getTarget()))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});
    } else {
      goldLinks = getPmEdges(pmPath)
          .map(edge -> new Tuple2<>(edge.f0, edge.f1))
          .returns(new TypeHint<Tuple2<Long, Long>>() {});
    }

    DataSet<Tuple2<Long, Long>> truePositives = goldLinks
        .join(clusterEdges)
        .where(0, 1).equalTo(0, 1)
        .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
            return first;
          }
        });

    long goldCount = goldLinks.count();
//    LOG.info("gold links: " + goldCount); // new execution
    long checkCount = clusterEdges.count();
    long tpCount = truePositives.count();

    double precision = (double) tpCount / checkCount; // tp / (tp + fp)
    double recall = (double) tpCount / goldCount; // tp / (fn + tp)
    LOG.info("\n############### dataset: " + dataset + " mergeThreshold: " + mergeThreshold + " simSortThreshold: " + simSortThreshold);
    LOG.info("TP+FN: " + goldCount);
    LOG.info("TP+FP: " + checkCount);
    LOG.info("TP: " + tpCount);

    LOG.info("precision: " + precision + " recall: " + recall
        + " F1: " + 2 * precision * recall / (precision + recall));
    LOG.info("######################################################");
  }

  /**
   * fix duplicate nc test
   */
  private DataSet<Edge<Long, NullValue>> getPmEdges(String graphPath) {
    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
    DataSet<Tuple2<Long, Long>> clsIds = Utils.getInputGraph(logicalGraph, Constants.NC, env)
        .getVertices()
        .map(vertex -> new Tuple2<>(vertex.getId(),
            (long) vertex.getValue().get("clsId")))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    return clsIds
        .groupBy(1)
        .reduceGroup(new AllEdgesCreateGroupReducer<>());
  }

  /**
   * read input, preprocessing, representative creation
   */
  @Test
  public void testMusicDataSim() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicBrainzTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> inputVertices =
        new CSVDataSource(path, vertexFileName, env)
            .getVertices()
            .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {
              @Override
              public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
                return vertex.getValue().getCcId() == 1L
                    || vertex.getValue().getCcId() == 2L
                    || vertex.getValue().getCcId() == 3L
                    || vertex.getValue().getCcId() == 4L
                    || vertex.getValue().getCcId() == 7L
                    || vertex.getValue().getCcId() == 9L
                    || vertex.getValue().getCcId() == 42L
                    || vertex.getValue().getCcId() == 58L;
              }
            });

    DataSet<Edge<Long, NullValue>> inputEdges = inputVertices
        .runOperation(new EdgeComputationOnVerticesForKeySelector(
            new CcIdKeySelector(),
            EdgeComputationStrategy.SIMPLE));

    Graph<Long, ObjectMap, ObjectMap> graph = Graph
        .fromDataSet(inputVertices, inputEdges, env)
//        .run(new BasicEdgeSimilarityComputation(Constants.MUSIC, env)); // working similarity run
        .run(new DefaultPreprocessing(DataDomain.MUSIC, env));

    DataSet<Vertex<Long, ObjectMap>> representatives = graph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.MUSIC, Constants.COSINE_TRIGRAM,0.7, env))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.MUSIC));

    assertEquals(11, representatives.count());

//    representatives.print();
  }

  /**
   * test music merge on simple playground example data
   */ // todo CHECK NOT WORKING
  @Test
  public void testMusicMerge() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String path = MusicBrainzTest.class
        .getResource("/data/musicbrainz/mergeAdv/").getFile();

    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(path, true, env)
            .getVertices()
            .map(x -> {
//              LOG.info("repr: " + x.toString());
              return x;
            })
            .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
            .runOperation(new MergeInitialization(DataDomain.MUSIC))
            .runOperation(new MergeExecution(
                DataDomain.MUSIC,
                Constants.COSINE_TRIGRAM,
                0.5,
                5,
                env));

    mergedVertices.print();
  }

  /**
   * detailed merge part test, not needed anymore?
   */
  @Test
  public void testMusicMergeFirstPart() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicBrainzTest.class
        .getResource("/data/musicbrainz/merge/").getFile();

    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(path, true, env)
            .getVertices()
            .runOperation(new MergeInitialization(DataDomain.MUSIC));

    DataSet<MergeMusicTuple> clusters = mergedVertices
        .map(new MergeMusicTupleCreator());

    SimilarityFunction<MergeMusicTriplet, MergeMusicTriplet> simFunction =
        new MergeMusicSimilarity(Constants.COSINE_TRIGRAM);

    SimilarityComputation<MergeMusicTriplet,
        MergeMusicTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeMusicTriplet,
        MergeMusicTriplet>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.5)
        .build();

    // initial working set
    DataSet<MergeMusicTriplet> initialWorkingSet = clusters
        .filter(new SourceCountRestrictionFilter<>(DataDomain.MUSIC, 5))
        .groupBy(10)
        .reduceGroup(new MergeMusicTripletCreator(5))
        .runOperation(similarityComputation);

    DataSet<Tuple2<Long, Long>> transitions = initialWorkingSet
        .flatMap(new TransitionElementsFlatMapFunction<>(DataDomain.MUSIC));

    initialWorkingSet.print();
    transitions.print();
    // no elements, need real test for testing NonChanged
    initialWorkingSet
      .runOperation(new WorksetNewClusterRemoveOperation<>(transitions))
      .print();
  }
}
