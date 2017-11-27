package org.mappinganalysis;

import org.apache.flink.api.common.functions.FilterFunction;
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
import org.junit.Test;
import org.mappinganalysis.corruption.EdgeCreateCorruptionFunction;
import org.mappinganalysis.corruption.EdgeRemoveCorruptionFunction;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.graph.utils.EdgeComputationStrategy;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
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
import org.mappinganalysis.util.functions.SmallEdgeIdFirstMapFunction;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

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
        .runOperation(new EdgeComputationVertexCcSet(
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
        .runOperation(new EdgeComputationVertexCcSet(
            new CcIdKeySelector(),
            EdgeComputationStrategy.SIMPLE));

    System.out.println(inputEdges.count());

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

//    DataSet<Edge<Long, NullValue>> edges =
//    System.out.println(edges.count());
    // 8526
  }

  @Test
  public void changeParamTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicBrainzTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";

    DataSet<Vertex<Long, ObjectMap>> inputVertices =
        new CSVDataSource(path, vertexFileName, env)
            .getVertices()//;
            .filter(vertex -> vertex.getValue().getCcId() < 50L)
            .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    DataSet<Edge<Long, NullValue>> inputEdges = inputVertices
        .runOperation(new EdgeComputationVertexCcSet(
            new CcIdKeySelector(),
            EdgeComputationStrategy.SIMPLE));

    Graph<Long, ObjectMap, ObjectMap> graph = Graph
        .fromDataSet(inputVertices, inputEdges, env)
        .run(new DefaultPreprocessing(DataDomain.MUSIC, env));

    graph.getVertices().print();
    // too low sims because of bad sim metric
//    graph.getEdges().print();

    /*
       representative creation
     */
    DataSet<Vertex<Long, ObjectMap>> representatives = graph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.MUSIC, 0.7, env))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.MUSIC));

//    representatives.print();

    // compute edges within representatives
//    DataSet<Edge<Long, NullValue>> edgeResultSet = representatives
//        .runOperation(new EdgeComputationVertexCcSet());
//        .map(new SmallEdgeIdFirstMapFunction()); // include in edge comp

//    edgeResultSet.print();

//    printQuality(inputVertices, edgeResultSet);

//    // merge
    DataSet<Vertex<Long, ObjectMap>> merged = representatives
        .runOperation(new MergeInitialization(DataDomain.MUSIC))
        .runOperation(new MergeExecution(DataDomain.MUSIC, 5, env));
//
    printQuality(inputVertices, merged
        .runOperation(new EdgeComputationVertexCcSet())
        .map(new SmallEdgeIdFirstMapFunction()));
  }

  private void printQuality(DataSet<Vertex<Long, ObjectMap>> inputVertices,
                            DataSet<Edge<Long, NullValue>> edgeResultSet) throws Exception {
    long checkCount = edgeResultSet.count();
//    assertEquals(199, checkCount);

    LOG.info("checkcount: " + checkCount);

//    DataSet<Edge<Long, NullValue>> goldEdges = inputVertices
//        .runOperation(new EdgeComputationVertexCcSet(
//            new CcIdKeySelector(),
//            EdgeComputationStrategy.ALL,
//            true));
//    long goldCount = goldEdges.count();
////    goldEdges.print();
//
//    DataSet<Edge<Long, NullValue>> truePositives = goldEdges.join(edgeResultSet)
//        .where(0, 1).equalTo(0, 1)
//        .with(new JoinFunction<Edge<Long, NullValue>, Edge<Long, NullValue>, Edge<Long, NullValue>>() {
//          @Override
//          public Edge<Long, NullValue> join(Edge<Long, NullValue> left, Edge<Long, NullValue> right) throws Exception {
//            return left;
//          }
//        })
//        .distinct();
//
//    long tpCount = truePositives.count();
//
//    double precision = (double) tpCount / checkCount;
//    double recall = (double) tpCount / goldCount;
//    LOG.info("###############");
//    LOG.info("Precision = tp count / check count = " + tpCount + " / " + checkCount + " = " + precision);
//    LOG.info("###############");
//    LOG.info("Recall = tp count / gold count = " + tpCount + " / " + goldCount + " = " + recall);
//    LOG.info("###############");
//    LOG.info("f1 = 2 * precision * recall / (precision + recall) = "
//        + 2 * precision * recall / (precision + recall));
//    LOG.info("###############");
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
        .runOperation(new EdgeComputationVertexCcSet(
            new CcIdKeySelector(),
            EdgeComputationStrategy.SIMPLE));

    Graph<Long, ObjectMap, ObjectMap> graph = Graph
        .fromDataSet(inputVertices, inputEdges, env)
//        .run(new BasicEdgeSimilarityComputation(Constants.MUSIC, env)); // working similarity run
        .run(new DefaultPreprocessing(DataDomain.MUSIC, env));

    DataSet<Vertex<Long, ObjectMap>> representatives = graph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.MUSIC, 0.7, env))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.MUSIC));

    assertEquals(11, representatives.count());

//    representatives.print();
  }

  /**
   * test music merge on simple playground example data
   */
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
            .runOperation(new MergeExecution(DataDomain.MUSIC, 5, env));

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
        new MergeMusicSimilarity();

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
