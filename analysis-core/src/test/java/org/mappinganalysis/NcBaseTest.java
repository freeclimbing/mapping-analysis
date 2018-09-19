package org.mappinganalysis;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeTriplet;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NcBaseTest {
  private static final Logger LOG = Logger.getLogger(NcBaseTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void gradoopInputTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    final String graphPath = NcBaseTest.class
        .getResource("/data/nc/5s2/").getFile();
    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    assertEquals(logicalGraph.getVertices().count(), graph.getVertices().count());
    assertEquals(logicalGraph.getEdges().count(), graph.getEdges().count());
  }

  @Test
  public void ncOneToManyTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    final String graphPath = NcBaseTest.class
        .getResource("/data/nc/5s2/").getFile();
    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    graph = graph.filterOnVertices(vertex -> vertex.getId() == 402924453L
        || vertex.getId() == 302154337L || vertex.getId() == 305140591L
        || vertex.getId() == 401728981L || vertex.getId() == 101728981L
        || vertex.getId() == 301728981L || vertex.getId() == 405140591L
        || vertex.getId() == 105140591L || vertex.getId() == 203831531L
        || vertex.getId() == 403232101L || vertex.getId() == 501728981L
        || vertex.getId() == 201728981L || vertex.getId() == 505140591L
        || vertex.getId() == 205140591L || vertex.getId() == 305576168L);

    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.85);

    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .run(new DefaultPreprocessing(config))
        .run(new TypeGroupBy(env))
        .run(new SimSort(config))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

    int count = 0;
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      count++;
      if (vertex.getId() == 101728981L || vertex.getId() == 105140591L) {
        assertEquals(5, vertex.getValue().getVerticesCount());
      } else if (vertex.getId() == 403232101L || vertex.getId() == 203831531L
          || vertex.getId() == 305576168L) {
        assertEquals(1, vertex.getValue().getVerticesCount());
      } else {
        assertEquals(2, vertex.getValue().getVerticesCount());
      }
    }

    assertEquals(6, count);
  }

  @Test
  public void maxTenSourcesTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.7);
    final String graphPath = NcBaseTest.class
        .getResource("/data/nc/10s4/").getFile();
    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
        .run(new DefaultPreprocessing(config));

    DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(config))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

    representatives.map(new MapFunction<Vertex<Long,ObjectMap>, Tuple2<Long, Integer>>() {
      @Override
      public Tuple2<Long, Integer> map(Vertex<Long, ObjectMap> value) throws Exception {
        return new Tuple2<>(value.getId(),
            value.getValue().getVerticesCount());
      }
    })
        .sortPartition(1, Order.DESCENDING)
        .setParallelism(1)
        .first(1)
        .map(cluster -> {
          assertEquals(10, cluster.f1.intValue());
          return cluster;
        })
        .returns(new TypeHint<Tuple2<Long, Integer>>() {})
        .collect();
  }

  /**
   * source 1 reduce to some vertices, correct 1:n removal
   */
  @Test
  public void tenSourceOneToManyTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.7);
    final String graphPath = NcBaseTest.class
        .getResource("/data/nc/10s1/").getFile();
    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    graph = graph.filterOnVertices(vertex -> vertex.getId() == 704781154L
        || vertex.getId() ==  207292666L || vertex.getId() ==
        1004781154L || vertex.getId() ==  407411403L || vertex.getId() ==
        603679966L || vertex.getId() ==  805238457L || vertex.getId() ==
        304781154L || vertex.getId() ==  706377526L || vertex.getId() ==
        302488074L || vertex.getId() ==  604781154L || vertex.getId() ==
        402064536L || vertex.getId() ==  1007339769L || vertex.getId() ==
        606486386L || vertex.getId() ==  504781154L || vertex.getId() ==
        804781154L || vertex.getId() ==
        306818652L || vertex.getId() ==  204781154L || vertex.getId() ==
        705230672L || vertex.getId() ==  903703303L || vertex.getId() ==
        104781154L || vertex.getId() ==  806252977L);

    Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
        .run(new DefaultPreprocessing(config));

    DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(config))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

    for (Vertex<Long, ObjectMap> cluster : representatives.collect()) {
      if (cluster.getId() == 104781154L) {
        assertEquals(8, cluster.getValue().getVerticesCount());
      }
    }
  }

  @Test
  public void suffixTEst() throws Exception {
    String result;

    double a = 0.7;
    Double atmp = a*100;
    result = String.valueOf(atmp.intValue());

    assertEquals("70", result);
  }

  /**
   * todo check blocking here?
   */
  @Test
  public void bigFileBlockingTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    final String graphPath = "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/saeedi/5p/2/";

    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    graph.getVertices().map(vertex -> {
      String artist = vertex.getValue().getArtist();
      String label = vertex.getValue().getLabel();

      if (artist.isEmpty() || label.isEmpty()) {
        LOG.info("<1: " + vertex.toString());
      }
      while (artist.length() <= 1) {
        artist = artist.concat(Constants.WHITE_SPACE);
      }
      while (label.length() <= 1) {
        label = label.concat(Constants.WHITE_SPACE);
      }

      return new Tuple2<>(Utils.getBlockingKey(
          BlockingStrategy.STANDARD_BLOCKING,
          Constants.MUSIC,
          label.substring(0, 2).concat(artist.substring(0, 2)),
          4),
          1);
    })
        .returns(new TypeHint<Tuple2<String, Integer>>() {})
        .groupBy(0)
        .aggregate(Aggregations.SUM, 1)
        .sortPartition(1, Order.ASCENDING)
        .setParallelism(1)
        .print();
  }

  /**
   * test for csimq paper with alieh
   */
  @Test
  public void csimqTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.7);
    String graphPath = NcBaseTest.class
        .getResource("/data/nc/csimq").getFile();

    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);

    Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
        .run(new DefaultPreprocessing(config));

    DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(config))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

    String reprOut = graphPath.concat("/output/s70/");
    Collection<Vertex<Long, ObjectMap>> resultList = Lists.newArrayList();
    representatives.output(new LocalCollectionOutputFormat<>(resultList));
    new JSONDataSink(reprOut, "repr")
        .writeVertices(representatives);
    env.execute();

    for (Vertex<Long, ObjectMap> cluster : resultList) {
      if (cluster.getId() == 0) {
        assertEquals(4, cluster.getValue().getVerticesCount());
      } else if (cluster.getId() == 9) {
        assertTrue(cluster.getValue().getVerticesList().contains(9L)
            || cluster.getValue().getVerticesList().contains(10L));
      }
    }
  }

  /** local execution **/
  @Test
  public void ncToFileTest() throws Exception {
    int sourcesCount = 10;
    env = TestBase.setupLocalEnvironment();
    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setMetric(Constants.JARO_WINKLER);

    List<String> sourceList = Lists.newArrayList(
//        "/data/nc/10s1/"
//        ,
//        "/data/nc/10s2/",
//        "/data/nc/10s4/"
//        ,
        "/data/nc/10s5/"
    );

    for (String dataset : sourceList) {
        for (int simFor = 70; simFor <= 70; simFor += 5) {
        double simSortThreshold = (double) simFor / 100;
        config.setSimSortSimilarity(simSortThreshold);

        final String graphPath = NcBaseTest.class
            .getResource(dataset).getFile();
        LOG.info(graphPath);
        LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
        Graph<Long, ObjectMap, NullValue> graph = Utils
            .getInputGraph(logicalGraph, Constants.NC, env);

        Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
            .run(new DefaultPreprocessing(config));

        DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
            .run(new TypeGroupBy(env))
            .run(new SimSort(config))
            .getVertices()
            .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

          String reprOut = graphPath.concat("/output/")
              + "s" + simFor + "/";
          new JSONDataSink(reprOut, "repr")
              .writeVertices(representatives);
          env.execute();

          for (int mergeFor = 80; mergeFor <= 80; mergeFor += 5) {
            double mergeThreshold = (double) mergeFor / 100;

            LOG.info("run: " + dataset);

            // get representative graph#

            DataSet<Vertex<Long, ObjectMap>> diskRepresentatives =
                new org.mappinganalysis.io.impl.json.JSONDataSource(
                    reprOut.concat("output/repr/"), true, env)
                    .getVertices();

            DataSet<Vertex<Long, ObjectMap>> merged = diskRepresentatives
                .runOperation(new MergeInitialization(DataDomain.NC))
                .runOperation(new MergeExecution(
                    DataDomain.NC,
                    Constants.JARO_WINKLER,
                    BlockingStrategy.STANDARD_BLOCKING,
                    mergeThreshold,
                    sourcesCount,
                    env));

            ArrayList<Vertex<Long, ObjectMap>> result = Lists.newArrayList();
            merged.output(new LocalCollectionOutputFormat<>(result));
            env.execute();
//            String reprOut = graphPath.concat("/output/m") + mergeFor
//                + "s" + simFor + "/";

            QualityUtils.printNcQuality(env.fromCollection(result),
                config,
                graphPath.concat("/output/"), // only for test
                "local",
                dataset); // jobName!?
          }
        }
    }
  }

  // read existing files and determine quality
  @Test
  @Deprecated
  public void qualityOnlyTest() throws Exception {
    int sourcesCount = 10;
    env = TestBase.setupLocalEnvironment();

    List<String> sourceList = Lists.newArrayList(
//        "1/",
      "2/",
        "3/"//, "5/"
    );

//    String graphPath =
//        "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/nentwig/nc/10parties/";
    String graphPath =
        "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/saeedi/";

    for (String dataset : sourceList) {

//      final String graphPath = NorthCarolinaVoterBaseTest.class
//          .getResource(dataset).getFile();
      String pmPath = graphPath.concat(dataset);
//      DataSet<Tuple2<Long, Long>> edges = getPmEdges(pmPath)
//          .map(edge -> new Tuple2<>(edge.f0, edge.f1))
//          .returns(new TypeHint<Tuple2<Long, Long>>() {});

      for (int mergeFor = 90; mergeFor <= 90; mergeFor += 5) {
        double mergeThreshold = (double) mergeFor / 100;

        for (int simFor = 65; simFor <= 65; simFor += 10) {
          double simSortThreshold = (double) simFor / 100;

//          String reprOut = pmPath.concat("/output/nc-decomposition-representatives-JW-s70/");
//          DataSet<Vertex<Long, ObjectMap>> repr =
//              new org.mappinganalysis.io.impl.json.JSONDataSource(
//                  reprOut, true, env)
////                  reprOut.concat("output/test/"), true, env)
//                  .getVertices();
//          printQuality(dataset.concat("repr"), mergeThreshold, simSortThreshold,
//              repr, pmPath, sourcesCount);

          /* for changing simsort sim */
//          String reprOut = graphPath.concat("/output/m") + mergeFor
//              + "s" + simFor + "/";

          /* for representatives only */
          String bdcluOut = pmPath
              .concat("/output/nc-decomposition-representatives-96-s") + simFor + "-ct/";
          DataSet<Vertex<Long, ObjectMap>> merged =
              new org.mappinganalysis.io.impl.json.JSONDataSource(
                  bdcluOut, true, env)
                  .getVertices();
          QualityUtils.printQuality(dataset, 0.0, simSortThreshold,
              merged, pmPath, sourcesCount, env);

          /* for fixed simsort sim */
//          bdcluOut = pmPath.concat("/output/nc-merged-clusters-m"
//              + mergeFor + "-SB-96-s" + simFor + "-ct/");
//          merged =
//              new org.mappinganalysis.io.impl.json.JSONDataSource(
//                  bdcluOut, true, env)
//                  .getVertices();
//          printQuality(dataset, mergeThreshold, simSortThreshold,
//              merged, pmPath, sourcesCount);
        }
      }
    }
  }

  /**
   * needs much time
   */
  @Test
  public void northCarolinaHolisticTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setMetric(Constants.COSINE_TRIGRAM);
    int sourcesCount = 10;
    List<String> sourceList = Lists.newArrayList(//"/data/nc/10s1/",
        "/data/nc/10s2/",
        "/data/nc/10s5/");

    for (String dataset : sourceList) {
      for (int mergeFor = 50; mergeFor <= 95; mergeFor+=5) {
        double mergeThreshold = (double) mergeFor / 100;

        for (int simFor = 95; simFor <= 95; simFor += 5) {
          double simSortThreshold = (double) simFor / 100;
          config.setSimSortSimilarity(simSortThreshold);

          final String graphPath = NcBaseTest.class
              .getResource(dataset).getFile();
          LogicalGraph logicalGraph = Utils
              .getGradoopGraph(graphPath, env);
          Graph<Long, ObjectMap, NullValue> graph = Utils
              .getInputGraph(logicalGraph, Constants.NC, env);

          DataSet<Vertex<Long, ObjectMap>> representatives = graph
              .run(new DefaultPreprocessing(config))
              .run(new TypeGroupBy(env))
              .run(new SimSort(config))
              .getVertices()
              .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

//    representatives.first(20).print();

          DataSet<Vertex<Long, ObjectMap>> merged = representatives
              .runOperation(new MergeInitialization(DataDomain.NC))
              .runOperation(new MergeExecution(
                  DataDomain.NC,
                  config.getMetric(),
                  mergeThreshold,
                  sourcesCount,
                  env));

//    merged.map(new MapFunction<Vertex<Long,ObjectMap>, Tuple3<Long, Integer, Set<Long>>>() {
//      @Override
//      public Tuple3<Long, Integer, Set<Long>> map(Vertex<Long, ObjectMap> value) throws Exception {
//        return new Tuple3<>(value.getId(), value.getValue().getVerticesCount(), value.getValue().getVerticesList());
//      }
//    }).sortPartition(1, Order.DESCENDING)
//        .setParallelism(1)
//        .first(50)
//        .print();

//    // 07677847s2 williams 207677847
//    // 01645993s5 willis 501645993
//    // 04737686s4 dickerson 404737686

          QualityUtils.printQuality(dataset, mergeThreshold, simSortThreshold,
              merged, Constants.EMPTY_STRING, sourcesCount, env);
        }
      }

    }

    assertEquals(1, 1);
  }


  /**
   * TODO really TEST this setting and check that most of the
   * TODO merges are done in the first iteration step
   */

  @Test
  public void staticNCTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    final String graphPath = NcBaseTest.class
              .getResource("/data/nc/10s2/").getFile();

//    final String graphPath = "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000" +
//        "/user/saeedi/5p/inputGraphs/config1_th0.7/";
    LogicalGraph logicalGraph = Utils
        .getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

//    graph = graph.filterOnVertices(vertex -> {
//          if (vertex.getValue().getNumber().startsWith("2861")) {
//            System.out.println(vertex.toString());
//            return true;
//          }
//          else {
//            return false;
//          }
//        });

//    System.out.println(
//        .count());

//    graph.filterOnVertices(x -> {
//      if (x.getValue().getNumber().startsWith("286"))
//    })

    IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
    config.setBlockingStrategy(BlockingStrategy.BLOCK_SPLIT);
    config.setMetric(Constants.COSINE_TRIGRAM);
    config.setSimSortSimilarity(0.5);
    config.setBlockingLength(6);
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

    QualityUtils.printNcQuality(env.fromCollection(representatives),
        config,
        graphPath,
        "local",
        "10");

    DataSet<Vertex<Long, ObjectMap>> resultingClusters = env
        .fromCollection(representatives)
        .runOperation(new MergeExecution(config));

    representatives = Lists.newArrayList();
    resultingClusters.output(new LocalCollectionOutputFormat<>(representatives));
    env.execute();

//    for (Vertex<Long, ObjectMap> representative : representatives) {
//      System.out.println(representative);
//    }

    QualityUtils.printNcQuality(env.fromCollection(representatives),
        config,
        graphPath,
        "local",
        "10");
  }


  @Test
  public void groupingTest() throws Exception {
    List<Tuple4<Integer, Integer, String, Double>> tupleList = Lists.newArrayList();

    tupleList.add(new Tuple4<>(10, 11, "def", 0.8));
    tupleList.add(new Tuple4<>(8, 9, "abc", 0.8));
    tupleList.add(new Tuple4<>(6, 7, "abc", 0.8));
    tupleList.add(new Tuple4<>(6, 7, "abc", 0.5));
    tupleList.add(new Tuple4<>(6, 7, "abc", 0.1));
    tupleList.add(new Tuple4<>(2, 3, "def", 0.85));
    tupleList.add(new Tuple4<>(12, 13, "abc", 0.85));
    tupleList.add(new Tuple4<>(0, 1, "abc", 0.8));

    tupleList.add(new Tuple4<>(4, 5, "abc", 0.7));

    env = TestBase.setupLocalEnvironment();

    DataSource<Tuple4<Integer, Integer, String, Double>> tuples = env.fromCollection(tupleList);

    System.out.println("### only max: ");

    // this is now wrong
    tuples.groupBy(2).max(3).andMin(0).andMin(1).print();
//
//    System.out.println("### only maxby: ");
//
//    tuples.groupBy(2).maxBy(3).print();
//
//    System.out.println("### maxby + join:");
//
//    tuples.join(tuples.groupBy(2).maxBy(3))
//        .where(2,3)
//        .equalTo(2,3)
//        .with(((first, second) -> first))
//        .returns(new TypeHint<Tuple4<Integer, Integer, String, Double>>() {})
//        .print();

    System.out.println("\n### max + join:");

    DataSet<Tuple4<Integer, Integer, String, Double>> tmpResult = tuples
        .join(tuples.groupBy(2).max(3))
        .where(2, 3)
        .equalTo(2, 3)
        .with(((first, second) -> first))
        .returns(new TypeHint<Tuple4<Integer, Integer, String, Double>>() {
        });

//    tuples.groupBy(2).reduce(
//        new ReduceFunction<Tuple4<Integer, Integer, String, Double>>() {
//      @Override
//      public Tuple4<Integer, Integer, String, Double> reduce(
//          Tuple4<Integer, Integer, String, Double> left,
//          Tuple4<Integer, Integer, String, Double> right) throws Exception {
//        if (left.f3.doubleValue() == right.f3) {
//          if (left.f0 < right.f0) {
//            return left;
//          } else if (left.f0 == right.f0.intValue()) {
//            if (left.f1 < right.f1) {
//              return left;
//            } else {
//              return right;
//            }
//          } else {
//            return right;
//          }
//        }
//        return left.f3 > right.f3 ? left : right;
//      }
//    }).print();


    DataSet<Tuple4<Integer, Integer, String, Double>> preResult = tmpResult
        .groupBy(2)
        .sortGroup(0, Order.ASCENDING)
        .sortGroup(1, Order.ASCENDING)
        .first(1);
//        .reduceGroup(new GroupReduceFunction<Tuple4<Integer, Integer, String, Double>,
//            Tuple4<Integer, Integer, String, Double>>() {
//          @Override
//          public void reduce(
//              Iterable<Tuple4<Integer, Integer, String, Double>> values,
//              Collector<Tuple4<Integer, Integer, String, Double>> out)
//              throws Exception {
//            HashSet<Integer> processedSet = Sets.newHashSet();
//            for (Tuple4<Integer, Integer, String, Double> value : values) {
//              System.out.println("processing: " + value.toString());
//
//              if (!processedSet.contains(value.f0)
//                  && !processedSet.contains(value.f1)) {
//                processedSet.add(value.f1);
//                processedSet.add(value.f0);
//
//                out.collect(value);
//              }
//            }
//          }
//        });


    preResult.print();
//    tuples.print();

  }

  /**
   * With Cosine Similarity, results look much better.
   */
  @Test
  public void mergeNcSimulateOneBlockTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    String metric = Constants.COSINE_TRIGRAM;

    List<Long> entryList = Lists.newArrayList();
    for (int i = 0; i < 20; ++i) {
      entryList.add((long) i);
    }
    DataSource<Long> longDataSource = env.fromCollection(entryList);

    DataSet<Vertex<Long, ObjectMap>> representatives = longDataSource
        .map(input -> {
          ObjectMap map = new ObjectMap(Constants.NC);
          long lastDigit = (input % 10);
          long tenDigit = (input / 10) % 10;
          String dataSource = "geco" + String.valueOf(lastDigit + 1);
          map.setClusterDataSources(Sets.newHashSet(dataSource));
          if (tenDigit != 0) {
            map.setLabel("foo" + tenDigit + tenDigit + tenDigit + input);
          } else {
            map.setLabel("foo" + tenDigit + tenDigit + tenDigit + tenDigit + input);
          }
          map.setClusterVertices(Sets.newHashSet(input));
          map.setAlbum(Constants.NO_VALUE);
          map.setArtist("bar");

          return new Vertex<>(input, map);
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {
        });

    DataSet<Vertex<Long, ObjectMap>> results = representatives
        .runOperation(new MergeExecution(
            DataDomain.NC,
            metric,
            BlockingStrategy.LSH_BLOCKING,
            0.7,
            10,
            env));

    results.print();
//    Set<Long> checkSet = Sets.newHashSet();
//    for (Vertex<Long, ObjectMap> longObjectMapVertex : results.collect()) {
//      for (Long aLong : longObjectMapVertex.getValue().getVerticesList()) {
//        assertTrue(checkSet.add(aLong));
////        if (!checkSet.add(aLong))
////          LOG.info("duplicate element: " + longObjectMapVertex.toString());
////        }
//      }
////      LOG.info(longObjectMapVertex.toString());
//    }
  }

  @Test
  public void asimTest() throws Exception {
    String foo = "foo11111";
    String bar = "foo11116";
    Double sim = Utils.getSimilarityAndSimplifyForMetric(foo, bar, Constants.COSINE_TRIGRAM);

    assertEquals(0.8969285, (sim + 1d) / 2, 0.001);
//    LOG.info((sim+1d)/2);

    foo = "foo33331";
    bar = "foo33337";
    sim = Utils.getSimilarityAndSimplifyForMetric(foo, bar, Constants.COSINE_TRIGRAM);

    assertEquals(0.875, (sim + 1d) / 2, 0.001);
//    LOG.info((sim+1d)/2);
  }

  @Test
  public void testIdSplit() throws Exception {
    String foo = "05802084s1";
    String result = Constants.EMPTY_STRING;

    for (String s : Splitter.on('s').split(foo)) {
      result = s.concat(result);
//      LOG.info(s);
    }

    assertEquals(105802084L, Long.parseLong(result));

    String ten = "05802084s10";
    result = Constants.EMPTY_STRING;

    for (String s : Splitter.on('s').split(ten)) {
      result = s.concat(result);
//      LOG.info(s);
    }

    assertEquals(1005802084L, Long.parseLong(result));
  }
}
