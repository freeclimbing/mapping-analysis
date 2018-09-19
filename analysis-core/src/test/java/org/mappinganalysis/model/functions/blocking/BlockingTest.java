package org.mappinganalysis.model.functions.blocking;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.benchmark.MusicbrainzBenchmarkTest;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.model.MergeTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.blocksplit.BlockSplitTripletCreator;
import org.mappinganalysis.model.functions.merge.MergeTupleCreator;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

public class BlockingTest {
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  @Test
  public void blockSplitDistributionTest() throws Exception {
    String inputPath =
        "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/" +
            "user/saeedi/10p/inputGraphs/initialGraph/";
    LogicalGraph logicalGraph = Utils
        .getGradoopGraph(inputPath, env);
    Utils
        .getInputGraph(logicalGraph, Constants.NC, env)
    .getVertices()
        .map(vertex -> {
          String blockingKey = Utils.getBlockingKey(
              BlockingStrategy.BLOCK_SPLIT,
              Constants.NC,
              Utils.getNcBlockingLabel(
                  vertex.getValue().getArtist(), vertex.getValue().getLabel(), 6),
              6);
          return new Tuple2<>(blockingKey, 1);
        })
        .returns(new TypeHint<Tuple2<String, Integer>>() {})
        .groupBy(0)
        .sum(1)
        .map(tuple -> new Tuple2<>(tuple.f1, 1))
        .returns(new TypeHint<Tuple2<Integer, Integer>>() {})
        .groupBy(0)
        .sum(1)
        .setParallelism(1)
        .sortPartition(0, Order.DESCENDING)
//        .first(1000)
        .print();



  }

  @Test
  public void getGraphTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/").getFile();
    final String vertexFileName = "basic/music-test.csv";
    Graph<Long, ObjectMap, NullValue> baseGraph
        = new CSVDataSource(path, vertexFileName, env)
        .getGraph();

    DataSet<MergeTriplet> triplets = baseGraph.getVertices()
        .map(new MergeTupleCreator(
            BlockingStrategy.STANDARD_BLOCKING,
            DataDomain.MUSIC,
            4))
        .runOperation(new BlockSplitTripletCreator());

    triplets.print();
  }

  @Test
  public void fullMusicBlockSplitTest() throws Exception {
    final String path = MusicbrainzBenchmarkTest.class
        .getResource("/data/musicbrainz/").getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    Graph<Long, ObjectMap, NullValue> baseGraph
        = new CSVDataSource(path, vertexFileName, env)
        .getGraph();

    DataSet<Vertex<Long, ObjectMap>> inVertices = baseGraph
        .getVertices()
        .filter(new SourceFilterFunction("1"))
        .union(baseGraph.getVertices().filter(new SourceFilterFunction("2")));

    DataSet<MergeTriplet> triplets = inVertices
        .map(new MergeTupleCreator(
            BlockingStrategy.STANDARD_BLOCKING,
            DataDomain.MUSIC,
            4))
        .runOperation(new BlockSplitTripletCreator());

    System.out.println(triplets.count());
    triplets.print();
    // output triplet looks like
    // (11646,11819,
    // 11646,Peter en de Wolf,Peter en de Wolf,Сергей Сергеевич Прокофьев,2004,1697,
    // no_or_minor_lang,[geco1],11646,сергей,true,,
    // 11819,Сергей Сергеевич Прокофьев - Peter en de Wolf,Peter en de Wolf,--,2004,1697,
    // dutch,[geco2],11819,сергей,true,,
    // 0.0,)
  }
}
