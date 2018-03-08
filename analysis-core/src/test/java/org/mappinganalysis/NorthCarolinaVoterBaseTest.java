package org.mappinganalysis;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.mappinganalysis.graph.utils.AllEdgesCreateGroupReducer;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.decomposition.representative.RepresentativeCreatorMultiMerge;
import org.mappinganalysis.model.functions.decomposition.simsort.SimSort;
import org.mappinganalysis.model.functions.decomposition.typegroupby.TypeGroupBy;
import org.mappinganalysis.model.functions.merge.MergeExecution;
import org.mappinganalysis.model.functions.merge.MergeInitialization;
import org.mappinganalysis.model.functions.preprocessing.DefaultPreprocessing;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NorthCarolinaVoterBaseTest {
  private static final Logger LOG = Logger.getLogger(NorthCarolinaVoterBaseTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  @Test
  public void gradoopInputTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    final String graphPath = NorthCarolinaVoterBaseTest.class
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
    final String graphPath = NorthCarolinaVoterBaseTest.class
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

    DataSet<Vertex<Long, ObjectMap>> vertices = graph
        .run(new DefaultPreprocessing(DataDomain.NC, env))
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.NC, Constants.COSINE_TRIGRAM,0.85, env))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

    int count = 0;
    for (Vertex<Long, ObjectMap> vertex : vertices.collect()) {
      count++;
      if (vertex.getId() == 101728981L) {
        assertEquals(5, vertex.getValue().getVerticesCount());
      } else if (vertex.getId() == 205140591L || vertex.getId() == 203831531L
          || vertex.getId() == 302154337L || vertex.getId() == 402924453L) {
        assertEquals(1, vertex.getValue().getVerticesCount());
      }
    }

    assertEquals(7, count);
  }

  private void printQuality(
      String dataset,
      double mergeThreshold,
      double simSortThreshold,
      DataSet<Vertex<Long, ObjectMap>> merged,
      String pmPath,
      int sourcesCount) throws Exception {
    DataSet<Tuple2<Long, Long>> clusterEdges = merged
        .flatMap(new QualityEdgeCreator());

    String path = "/data/nc/" + sourcesCount + "pm/";
    DataSet<Tuple2<Long, Long>> goldLinks;

    if (pmPath.equals(Constants.EMPTY_STRING)) {
      pmPath = NorthCarolinaVoterBaseTest.class
          .getResource(path).getFile();

      DataSet<Tuple2<String, String>> perfectMapping = env
          .readCsvFile(pmPath.concat("pm.csv"))
          .types(String.class, String.class);

      goldLinks = perfectMapping
          .map(new MapFunction<Tuple2<String, String>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Tuple2<String, String> pmValue) throws Exception {
              long first = Utils.getIdFromNcId(pmValue.f0);
              long second = Utils.getIdFromNcId(pmValue.f1);

              if (first < second) {
                return new Tuple2<>(first, second);
              } else {
                return new Tuple2<>(second, first);
              }
            }
          });
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
    long checkCount = clusterEdges.count();
    long tpCount = truePositives.count();

    double precision = (double) tpCount / checkCount;
    double recall = (double) tpCount / goldCount;
    LOG.info("\n############### dataset: " + dataset + " mergeThreshold: " + mergeThreshold + " simSortThreshold: " + simSortThreshold);
    LOG.info("TP+FN: " + goldCount);
    LOG.info("TP+FP: " + checkCount);
    LOG.info("TP: " + tpCount);

    LOG.info("precision: " + precision + " recall: " + recall
        + " F1: " + 2 * precision * recall / (precision + recall));
    LOG.info("######################################################");
  }

  @Test
  public void maxTenSourcesTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    final String graphPath = NorthCarolinaVoterBaseTest.class
        .getResource("/data/nc/10s4/").getFile();
    LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
    Graph<Long, ObjectMap, NullValue> graph = Utils
        .getInputGraph(logicalGraph, Constants.NC, env);

    Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
        .run(new DefaultPreprocessing(DataDomain.NC, env));

    DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.NC, Constants.COSINE_TRIGRAM,0.7, env))
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
    final String graphPath = NorthCarolinaVoterBaseTest.class
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
        .run(new DefaultPreprocessing(DataDomain.NC, env));

    DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.NC, Constants.COSINE_TRIGRAM,0.7, env))
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
          label.substring(0, 2).concat(artist.substring(0, 2))
      ), 1);
    })
        .returns(new TypeHint<Tuple2<String, Integer>>() {})
        .groupBy(0)
        .aggregate(Aggregations.SUM, 1)
        .sortPartition(1, Order.ASCENDING)
        .setParallelism(1)
        .print();
  }

  @Test
  public void csimqTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    String graphPath = NorthCarolinaVoterBaseTest.class
        .getResource("/data/nc/csimq").getFile();

    Graph<Long, ObjectMap, NullValue> graph =
        new JSONDataSource(graphPath, true, env)
            .getGraph(ObjectMap.class, NullValue.class);
//            .run(new BasicEdgeSimilarityComputation(
//                Constants.COSINE_TRIGRAM, Constants.NC, env));

//    graph.getEdges().print();
    Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
        .run(new DefaultPreprocessing(Constants.COSINE_TRIGRAM, DataDomain.NC, env));

    DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.NC, Constants.COSINE_TRIGRAM, 0.7, env))
        .getVertices()
        .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

    String reprOut = graphPath.concat("/output/s70/");
    new JSONDataSink(reprOut, "repr")
        .writeVertices(representatives);
    env.execute();

    representatives.print();

  }

  /** local execution **/
  @Test
  public void ncToFileTest() throws Exception {
    int sourcesCount = 10;
    env = TestBase.setupLocalEnvironment();
    String metric = Constants.JARO_WINKLER;
    LOG.info(env.getParallelism());

    List<String> sourceList = Lists.newArrayList("/data/nc/10s1/"
        ,
        "/data/nc/10s2/",
        "/data/nc/10s4/"
        ,
        "/data/nc/10s5/"
    );

    for (String dataset : sourceList) {
        for (int simFor = 70; simFor <= 70; simFor += 5) {
        double simSortThreshold = (double) simFor / 100;

        final String graphPath = NorthCarolinaVoterBaseTest.class
            .getResource(dataset).getFile();
        LogicalGraph logicalGraph = Utils.getGradoopGraph(graphPath, env);
        Graph<Long, ObjectMap, NullValue> graph = Utils
            .getInputGraph(logicalGraph, Constants.NC, env);

        Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
            .run(new DefaultPreprocessing(metric, DataDomain.NC, env));

        DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
            .run(new TypeGroupBy(env))
            .run(new SimSort(DataDomain.NC, metric, simSortThreshold, env))
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
                    metric,
                    BlockingStrategy.STANDARD_BLOCKING,
                    mergeThreshold,
                    sourcesCount,
                    env));

            String finalPath = graphPath.concat("output/m") + mergeFor
                + "s" + simFor + "/";
            new JSONDataSink(finalPath, "merge")
                .writeVertices(merged);
            env.execute();

//            String reprOut = graphPath.concat("/output/m") + mergeFor
//                + "s" + simFor + "/";

            DataSet<Vertex<Long, ObjectMap>> diskMerged =
                new org.mappinganalysis.io.impl.json.JSONDataSource(
                    finalPath.concat("output/merge/"), true, env)
                    .getVertices();

            printQuality(dataset, mergeThreshold, simSortThreshold,
                diskMerged, Constants.EMPTY_STRING, sourcesCount);
//        printQuality(dataset, mergeThreshold, simSortThreshold, merged, sourcesCount);
          }
        }
    }
  }

  @Test
  public void bigNcQualityTest() throws Exception {
    int sourcesCount = 10;
    env = TestBase.setupLocalEnvironment();

    String graphPath = "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/saeedi/1/";
    DataSet<Edge<Long, NullValue>> edges = getPmEdges(graphPath);

    edges.first(10).print();

  }

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

  // read existing files and determine quality
  @Test
  public void qualityOnlyTest() throws Exception {
    int sourcesCount = 5;
    env = TestBase.setupLocalEnvironment();

    List<String> sourceList = Lists.newArrayList(
//        "1/"//,
 "2/", "3/"//, "5/"
    );

//    String graphPath =
//        "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/nentwig/nc/10parties/";
    String graphPath =
        "hdfs://bdclu1.informatik.intern.uni-leipzig.de:9000/user/saeedi/5p/";


    for (String dataset : sourceList) {
//      final String graphPath = NorthCarolinaVoterBaseTest.class
//          .getResource(dataset).getFile();
      String pmPath = graphPath.concat(dataset);
//      DataSet<Tuple2<Long, Long>> edges = getPmEdges(pmPath)
//          .map(edge -> new Tuple2<>(edge.f0, edge.f1))
//          .returns(new TypeHint<Tuple2<Long, Long>>() {});

      for (int mergeFor = 95; mergeFor <= 95; mergeFor += 5) {
        double mergeThreshold = (double) mergeFor / 100;

        for (int simFor = 70; simFor <= 70; simFor += 5) {
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
          /* for fixed simsort sim */
//          String bdcluOut = pmPath.concat("/output/nc-merged-clusters-m"
//              + mergeFor + "-SB-96-s70-jw/");
          /* for representatives only */
          String bdcluOut = pmPath
              .concat("/output/nc-decomposition-representatives-96-s70-ct/");
          DataSet<Vertex<Long, ObjectMap>> merged =
              new org.mappinganalysis.io.impl.json.JSONDataSource(
                  bdcluOut, true, env)
                  .getVertices();
          printQuality(dataset, mergeThreshold, simSortThreshold,
              merged, pmPath, sourcesCount);
        }
      }
    }
  }

  @Test
  public void northCarolinaHolisticTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    int sourcesCount = 10;
    final String metric = Constants.COSINE_TRIGRAM;
    List<String> sourceList = Lists.newArrayList(//"/data/nc/10s1/",
        "/data/nc/10s2/",
        "/data/nc/10s5/");
    for (String dataset : sourceList) {
      for (int mergeFor = 50; mergeFor <= 95; mergeFor+=5) {
        double mergeThreshold = (double) mergeFor / 100;

        for (int simFor = 95; simFor <= 95; simFor += 5) {
          double simSortThreshold = (double) simFor / 100;

          final String graphPath = NorthCarolinaVoterBaseTest.class
              .getResource(dataset).getFile();
          LogicalGraph logicalGraph = Utils
              .getGradoopGraph(graphPath, env);
          Graph<Long, ObjectMap, NullValue> graph = Utils
              .getInputGraph(logicalGraph, Constants.NC, env);

          DataSet<Vertex<Long, ObjectMap>> representatives = graph
              .run(new DefaultPreprocessing(metric, DataDomain.NC, env))
              .run(new TypeGroupBy(env))
              .run(new SimSort(DataDomain.NC, metric, simSortThreshold, env))
              .getVertices()
              .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

//    representatives.first(20).print();

          DataSet<Vertex<Long, ObjectMap>> merged = representatives
              .runOperation(new MergeInitialization(DataDomain.NC))
              .runOperation(new MergeExecution(
                  DataDomain.NC,
                  metric,
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

          printQuality(dataset, mergeThreshold, simSortThreshold,
              merged, Constants.EMPTY_STRING, sourcesCount);
        }
      }

    }

    assertEquals(1, 1);
  }

  /**
   * With Cosine Similarity, results look much better.
   */
  @Test
  public void mergeNcSimulateOneBlockTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    TestBase.setupConstants();
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

  private static class QualityEdgeCreator
      implements FlatMapFunction<Vertex<Long, ObjectMap>, Tuple2<Long, Long>> {
    @Override
    public void flatMap(Vertex<Long, ObjectMap> cluster, Collector<Tuple2<Long, Long>> out) throws Exception {
      Set<Long> firstSide = cluster.getValue().getVerticesList();
      Set<Long> secondSide = Sets.newHashSet(firstSide);
      for (Long first : firstSide) {
        secondSide.remove(first);
        for (Long second : secondSide) {
          if (first < second) {
            out.collect(new Tuple2<>(first, second));
          } else {
            out.collect(new Tuple2<>(second, first));
          }
        }
      }
    }
  }
}
