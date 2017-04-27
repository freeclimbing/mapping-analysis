package org.mappinganalysis;

import org.apache.flink.api.common.functions.FilterFunction;
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
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.graph.utils.EdgeComputationVertexCcSet;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeMusicTriplet;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreator;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.*;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

/**
 * Created by markus on 4/25/17.
 */
public class MusicBrainzTest {
  private static final Logger LOG = Logger.getLogger(MusicBrainzTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  /**
   * TMP test remove later
   * @throws Exception
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
        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector(), false));

    Graph<Long, ObjectMap, ObjectMap> graph = Graph.fromDataSet(inputVertices, inputEdges, env)
//        .run(new BasicEdgeSimilarityComputation(Constants.MUSIC, env)); // working similarity run
        .run(new DefaultPreprocessing(DataDomain.MUSIC, env));

//    String graphPath = SimSortTest.class.getResource("/data/musicbrainz/simsort/").getFile();
//    Graph<Long, ObjectMap, ObjectMap> graph =
//        new JSONDataSource(graphPath, true, env)
//            .getGraph();

    DataSet<Vertex<Long, ObjectMap>> vertices =
        graph
            .run(new TypeGroupBy(env)) // not needed? TODO
            .run(new SimSort(DataDomain.MUSIC, 0.7, env))
            .getVertices()
            .runOperation(new RepresentativeCreator(DataDomain.MUSIC));

//    vertices.collect();
    vertices.print();
  }

  @Test
  public void testMusicMerge() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String path = MusicBrainzTest.class
        .getResource("/data/musicbrainz/merge/")
        .getFile();

    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(path, true, env)
            .getVertices()
            .map(x -> {
              LOG.info("input vertex: " + x.toString());
              return x;
            })
            .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
            .runOperation(new MergeInitialization(DataDomain.MUSIC))
            .runOperation(new MergeExecution(DataDomain.MUSIC, 5));

    mergedVertices.print();
  }

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
    transitions.print();
//    initialWorkingSet.print();
  }
}
