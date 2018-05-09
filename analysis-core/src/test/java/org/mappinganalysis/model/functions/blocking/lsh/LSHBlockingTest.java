package org.mappinganalysis.model.functions.blocking.lsh;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.IncrementalClusteringTest;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.preprocessing.utils.InternalTypeMapFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import static org.junit.Assert.assertEquals;

public class LSHBlockingTest {
  private static final Logger LOG = Logger.getLogger(LSHBlockingTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  /* with LSH blocking we only achieve 25% recall instead of ~71% with SB */
  @Test
  public void lshSingleSourceVariationTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    final Graph<Long, ObjectMap, NullValue> baseGraph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class)
            .mapVertices(new InternalTypeMapFunction());

    DataSet<Vertex<Long, ObjectMap>> nytSource = baseGraph.getVertices()
        .filter(new SourceFilterFunction(Constants.NYT_NS));
    DataSet<Vertex<Long, ObjectMap>> dbpSource = baseGraph.getVertices()
        .filter(new SourceFilterFunction(Constants.DBP_NS));
    DataSet<Vertex<Long, ObjectMap>> fbSource = baseGraph.getVertices()
        .filter(new SourceFilterFunction(Constants.FB_NS));

    IncrementalConfig config = new IncrementalConfig(DataDomain.GEOGRAPHY, env);
    config.setBlockingStrategy(BlockingStrategy.LSH_BLOCKING);
    config.setStrategy(IncrementalClusteringStrategy.SINGLE_SETTING);
    config.setMetric(Constants.COSINE_TRIGRAM);

    IncrementalClustering.IncrementalClusteringBuilder baseBuilder =
        new IncrementalClustering
            .IncrementalClusteringBuilder(config);

    IncrementalClustering nytClustering = baseBuilder
        .setMatchElements(nytSource)
        .setNewSource(Constants.NYT_NS)
        .setDataSources(Sets.newHashSet(Constants.GN_NS, Constants.NYT_NS))
        .build();

    Graph<Long, ObjectMap, NullValue> workGraph = baseGraph
        .filterOnVertices(new SourceFilterFunction(Constants.GN_NS));
    DataSet<Vertex<Long, ObjectMap>> clusters = workGraph.run(nytClustering);

    new JSONDataSink(graphPath.concat("/gn-nyt/"), "test")
        .writeVertices(clusters);
    env.execute();

    // second step, merge dbp
    workGraph = new JSONDataSource(
        graphPath.concat("/gn-nyt/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class)
        .mapVertices(new InternalTypeMapFunction());

    IncrementalClustering dbpClustering = baseBuilder
        .setMatchElements(dbpSource)
        .setNewSource(Constants.DBP_NS)
        .setDataSources(Sets.newHashSet(Constants.GN_NS, Constants.NYT_NS, Constants.DBP_NS))
        .build();

    clusters = workGraph.run(dbpClustering);

    new JSONDataSink(graphPath.concat("/gn-nyt-dbp/"), "test")
        .writeVertices(clusters);
    env.execute();

    // third step, merge fb
    workGraph = new JSONDataSource(
        graphPath.concat("/gn-nyt-dbp/output/test/"), "test", true, env)
        .getGraph(ObjectMap.class, NullValue.class)
        .mapVertices(new InternalTypeMapFunction());

    IncrementalClustering fbClustering = baseBuilder
        .setMatchElements(fbSource)
        .setNewSource(Constants.FB_NS)
        .setDataSources(Sets.newHashSet(Constants.GN_NS, Constants.NYT_NS, Constants.DBP_NS, Constants.FB_NS))
        .build();

    clusters = workGraph.run(fbClustering);

    new JSONDataSink(graphPath.concat("/gn-nyt-dbp-fb/"), "test")
        .writeVertices(clusters);
    env.execute();

    QualityUtils.printGeoQuality(clusters, config);
//    clusters.print();
//    assertEquals(2387L,clusters.count());
  }

  @Test
  public void lshIdfOptBlockingTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();
    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

//    graph = graph.filterOnVertices(x ->
//        x.getId() == 2479L || x.getId() == 2478L || x.getId() == 3640L);

    IncrementalClustering clustering = new IncrementalClustering
        .IncrementalClusteringBuilder()
        .setEnvironment(env)
        .setStrategy(IncrementalClusteringStrategy.FIXED_SEQUENCE)
        .setBlockingStrategy(BlockingStrategy.LSH_BLOCKING)
        .build();

    DataSet<Vertex<Long, ObjectMap>> clusters = graph
        .run(clustering);

    // check that all vertices are contained
    DataSet<Tuple2<Long, Long>> singleClusteredVertices = clusters
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>>() {
          @Override
          public void flatMap(Vertex<Long, ObjectMap> cluster, Collector<Tuple2<Long, Long>> out) throws Exception {
            for (Long vertex : cluster.getValue().getVerticesList()) {
              out.collect(new Tuple2<>(cluster.getId(), vertex));
            }
          }
        })
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    DataSet<Vertex<Long, ObjectMap>> with = graph.getVertices()
        .leftOuterJoin(singleClusteredVertices)
        .where(0)
        .equalTo(1)
        .with(new LeftMinusRightSideJoinFunction<>());

    with.print(); // TODO still missing vertices
//    LOG.info("too much: " + with.count());
//
//    LOG.info("vertices in final Clusters: " + singleClusteredVertices.count()); // 3074
//    LOG.info("distinct: " + singleClusteredVertices.distinct(1).count()); // 3054
//    LOG.info("orig verts: " + graph.getVertices().count()); //3054
//    new JSONDataSink(graphPath.concat("/output-lsh-opt-blocking/"), "test")
//        .writeVertices(clusteredVertices);
//    env.execute();
//

//    clusters.filter(cluster -> cluster.getValue().getVerticesList().contains(742L)
//        || cluster.getValue().getVerticesList().contains(3644L)
//        || cluster.getValue().getVerticesList().contains(2345L))
//        .print();

    // check that no vertex contained in the clustered vertices is duplicated
    // TODO duplicates seem to be ok, all checked duplicates overlap and should be in one cluster
    // TODO still need to check why they are not in one cluster
    // TODO LSH candidates seem to be bugged!?
    DataSet<Tuple3<Long, Long, Integer>> sum = clusters
        .map(cluster -> {
//          if (cluster.getValue().getVerticesList().contains(6730L)
//              || cluster.getValue().getVerticesList().contains(2142L)
//              || cluster.getValue().getVerticesList().contains(5499L)) {
//            LOG.info("final out: " + cluster.toString());
//          }
//        if (cluster.getValue().getVerticesList().contains(298L)
//              || cluster.getValue().getVerticesList().contains(299L)
//              || cluster.getValue().getVerticesList().contains(5013L)
//              || cluster.getValue().getVerticesList().contains(5447L)) {
//            LOG.info("final out: " + cluster.toString());
//          }
          return cluster;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .flatMap(new FlatMapFunction<Vertex<Long, ObjectMap>, Tuple3<Long, Long, Integer>>() {
          @Override
          public void flatMap(Vertex<Long, ObjectMap> value, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            for (Long aLong : value.getValue().getVerticesList()) {
              out.collect(new Tuple3<>(value.getId(), aLong, 1));
            }
          }
        })
        .groupBy(1)
        .reduceGroup(new GroupReduceFunction<Tuple3<Long,Long,Integer>, Tuple3<Long, Long, Integer>>() {
          @Override
          public void reduce(Iterable<Tuple3<Long, Long, Integer>> values, Collector<Tuple3<Long, Long, Integer>> out) throws Exception {
            Tuple3<Long, Long, Integer> result = null;
            for (Tuple3<Long, Long, Integer> value : values) {
              if (result == null) {
                result = value;
              } else {
                LOG.info(value.toString());
                result.f2 += value.f2;
              }
            }
            out.collect(result);

          }
        });
//        .sum(2);

    assertEquals(0, sum.filter(tuple -> tuple.f2 > 1).count());
  }
}
