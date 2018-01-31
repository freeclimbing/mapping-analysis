package org.mappinganalysis;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Test;
import org.mappinganalysis.graph.utils.GradoopEdgeToGellyEdgeMapper;
import org.mappinganalysis.graph.utils.GradoopToGellyEdgeJoinFunction;
import org.mappinganalysis.graph.utils.GradoopToObjectMapVertexMapper;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.ObjectMap;
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
    LogicalGraph logicalGraph = getGradoopGraph(graphPath);
    Graph<Long, ObjectMap, NullValue> graph = getInputGraph(logicalGraph);

    assertEquals(logicalGraph.getVertices().count(), graph.getVertices().count());
    assertEquals(logicalGraph.getEdges().count(), graph.getEdges().count());
  }

  @Test
  public void ncOneToManyTest() throws Exception {
    env = TestBase.setupLocalEnvironment();
    final String graphPath = NorthCarolinaVoterBaseTest.class
        .getResource("/data/nc/5s2/").getFile();
    LogicalGraph logicalGraph = getGradoopGraph(graphPath);
    Graph<Long, ObjectMap, NullValue> graph = getInputGraph(logicalGraph);

    graph = graph.filterOnVertices(vertex -> vertex.getId() == 402924453L
        || vertex.getId() == 302154337L || vertex.getId() == 305140591L
        || vertex.getId() == 401728981L || vertex.getId() == 101728981L
        || vertex.getId() == 301728981L || vertex.getId() == 405140591L
        || vertex.getId() == 105140591L || vertex.getId() == 203831531L
        || vertex.getId() == 403232101L || vertex.getId() == 501728981L
        || vertex.getId() == 201728981L || vertex.getId() == 505140591L
        || vertex.getId() == 205140591L || vertex.getId() == 305576168L);

    Graph<Long, ObjectMap, ObjectMap> simGraph = graph
        .run(new DefaultPreprocessing(DataDomain.NC, env));

    Graph<Long, ObjectMap, ObjectMap> decompGraph = simGraph
        .run(new TypeGroupBy(env))
        .run(new SimSort(DataDomain.NC, 0.85, env));

    DataSet<Vertex<Long, ObjectMap>> vertices = decompGraph
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

  @Test
  public void tenSourceNCTest() throws Exception {
    int sourcesCount = 10;
    List<String> sourceList = Lists.newArrayList("/data/nc/10s1/"
//        ,
//        "/data/nc/10s2/",
//        "/data/nc/10s4/"
//        ,
//        "/data/nc/10s5/"
    );
    for (String dataset : sourceList) {
      for (int mergeFor = 80; mergeFor <= 85; mergeFor += 5) {
        double mergeThreshold = (double) mergeFor / 100;

        for (int simFor = 60; simFor <= 70; simFor += 5) {
          double simSortThreshold = (double) simFor / 100;

          env = TestBase.setupLocalEnvironment();
          final String graphPath = NorthCarolinaVoterBaseTest.class
              .getResource(dataset).getFile();
          LogicalGraph logicalGraph = getGradoopGraph(graphPath);
          Graph<Long, ObjectMap, NullValue> graph = getInputGraph(logicalGraph);

//    graph = graph.filterOnVertices(vertex -> vertex.getId() == 906445785L
//        || vertex.getId() == 506445785L || vertex.getId() == 706445785L
//        || vertex.getId() == 606445785L || vertex.getId() == 406445785L
//        || vertex.getId() == 206445785L || vertex.getId() == 806445785L
//        || vertex.getId() == 106445785L || vertex.getId() == 306445785L
//        || vertex.getId() == 301865004L || vertex.getId() == 701865004L
//        || vertex.getId() == 501865004L || vertex.getId() == 101865004L
//        || vertex.getId() == 401865004L || vertex.getId() == 901865004L
//        || vertex.getId() == 801865004L || vertex.getId() == 1001865004L);

          // 906445785L, 506445785LL, 706445785L, 606445785L,406445785L,
          // 206445785L, 806445785L,106445785L, 306445785L

          // 301865004L, 701865004L,501865004L,101865004L,
          // 801865004L,1001865004L,401865004L, 901865004L

          DataSet<Vertex<Long, ObjectMap>> representatives = graph
              .run(new DefaultPreprocessing(DataDomain.NC, env))
              .run(new TypeGroupBy(env))
              .run(new SimSort(DataDomain.NC, simSortThreshold, env))
              .getVertices()
              .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

          DataSet<Vertex<Long, ObjectMap>> merged = representatives
              .runOperation(new MergeInitialization(DataDomain.NC))
              .runOperation(new MergeExecution(
                  DataDomain.NC,
                  mergeThreshold,
                  sourcesCount,
                  env));

          printQuality(dataset, mergeThreshold, simSortThreshold, merged, sourcesCount);
        }
      }
    }

  }

  private void printQuality(
      String dataset,
      double mergeThreshold,
      double simSortThreshold,
      DataSet<Vertex<Long, ObjectMap>> merged,
      int sourcesCount) throws Exception {
    DataSet<Tuple2<Long, Long>> clusterEdges = merged
        .flatMap(new QualityEdgeCreator());

    String path = "/data/nc/" + sourcesCount + "pm/";
    String pmPath = NorthCarolinaVoterBaseTest.class
        .getResource(path).getFile();

    DataSet<Tuple2<String, String>> perfectMapping = env
        .readCsvFile(pmPath.concat("pm.csv"))
        .types(String.class, String.class);

    DataSet<Tuple2<Long, Long>> goldLinks = perfectMapping
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

    DataSet<Tuple2<Long, Long>> truePositives = goldLinks.join(clusterEdges)
        .where(0, 1).equalTo(0, 1)
        .with(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
            return first;
          }
        });


//       truePositives.groupBy(0,1)
//        .reduceGroup(new GroupReduceFunction<Tuple2<Long,Long>, Tuple2<Long, Long>>() {
//          @Override
//          public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) throws Exception {
//            int count = 0;
//            for (Tuple2<Long, Long> value : values) {
//              if (count != 0) {
//                LOG.info(value.toString());
//              }
//              count++;
//              out.collect(value);
//            }
//          }
//        }).collect();

    long goldCount = perfectMapping.count();
    long checkCount = clusterEdges.count();
    long tpCount = truePositives.count();

    double precision = (double) tpCount / checkCount;
    double recall = (double) tpCount / goldCount;
    LOG.info("\n############### dataset: " + dataset + " mergeThreshold: " + mergeThreshold + " simSortThreshold: " + simSortThreshold);
//        LOG.info("Precision = tp count / check count = " + tpCount + " / " + checkCount + " = " + precision);
//        LOG.info("###############");
//        LOG.info("Recall = tp count / gold count = " + tpCount + " / " + goldCount + " = " + recall);
//        LOG.info("###############");
//        LOG.info("f1 = 2 * precision * recall / (precision + recall) = "
//            + 2 * precision * recall / (precision + recall));
//        LOG.info("\ngold links size (TP+FN): " + goldCount);
    LOG.info("TP+FP: " + checkCount);
    LOG.info("TP: " + tpCount);

    LOG.info("######################################################");
  }

  @Test
  public void tenSourceSmallTest() throws Exception {
    int sourcesCount = 10;
    List<String> sourceList = Lists.newArrayList("/data/nc/10s1/"
//        ,
//        "/data/nc/10s2/",
//        "/data/nc/10s4/"
//        ,
//        "/data/nc/10s5/"
    );
    for (String dataset : sourceList) {
      for (int mergeFor = 80; mergeFor <= 85; mergeFor += 5) {
        double mergeThreshold = (double) mergeFor / 100;

//        for (int simFor = 60; simFor <= 70; simFor += 5) {
        double simSortThreshold = 0.7;
//              (double) simFor / 100;

        env = TestBase.setupLocalEnvironment();
        final String graphPath = NorthCarolinaVoterBaseTest.class
            .getResource(dataset).getFile();
        LogicalGraph logicalGraph = getGradoopGraph(graphPath);
        Graph<Long, ObjectMap, NullValue> graph = getInputGraph(logicalGraph);

        Graph<Long, ObjectMap, ObjectMap> preprocGraph = graph
            .run(new DefaultPreprocessing(DataDomain.NC, env));

        preprocGraph = preprocGraph.filterOnVertices(vertex -> vertex.getId() == 704781154L
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

        preprocGraph.getVertices().print();

        DataSet<Vertex<Long, ObjectMap>> representatives = preprocGraph
            .run(new TypeGroupBy(env))
            .run(new SimSort(DataDomain.NC, simSortThreshold, env))
            .getVertices()
            .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

        representatives.map(new MapFunction<Vertex<Long,ObjectMap>, Tuple3<Long, Integer, Set<Long>>>() {
          @Override
          public Tuple3<Long, Integer, Set<Long>> map(Vertex<Long, ObjectMap> value) throws Exception {
            return new Tuple3<>(value.getId(), value.getValue().getVerticesCount(), value.getValue().getVerticesList());
          }
        }).sortPartition(1, Order.DESCENDING)
            .setParallelism(1)
            .first(50)
            .print();

        DataSet<Vertex<Long, ObjectMap>> merged = representatives
            .runOperation(new MergeInitialization(DataDomain.NC))
            .runOperation(new MergeExecution(
                DataDomain.NC,
                mergeThreshold,
                sourcesCount,
                env));


//        merged.map(new MapFunction<Vertex<Long,ObjectMap>, Tuple3<Long, Integer, Set<Long>>>() {
//          @Override
//          public Tuple3<Long, Integer, Set<Long>> map(Vertex<Long, ObjectMap> value) throws Exception {
//            return new Tuple3<>(value.getId(), value.getValue().getVerticesCount(), value.getValue().getVerticesList());
//          }
//        }).sortPartition(1, Order.DESCENDING)
//            .setParallelism(1)
//            .first(50)
//            .print();
      }
    }
  }

  @Test
  public void northCarolinaHolisticTest() throws Exception {

    int sourcesCount = 5;
    List<String> sourceList = Lists.newArrayList(
//        "/data/nc/5s2/",
//        "/data/nc/5s4/",
        "/data/nc/5s4/");
    for (String dataset : sourceList) {
      for (int mergeFor = 75; mergeFor <= 95; mergeFor+=5) {
        double mergeThreshold = (double) mergeFor / 100;

        for (int simFor = 50; simFor <= 95; simFor += 5) {
          double simSortThreshold = (double) simFor / 100;

          env = TestBase.setupLocalEnvironment();
          final String graphPath = NorthCarolinaVoterBaseTest.class
              .getResource(dataset).getFile();
          LogicalGraph logicalGraph = getGradoopGraph(graphPath);
          Graph<Long, ObjectMap, NullValue> graph = getInputGraph(logicalGraph);

          Graph<Long, ObjectMap, ObjectMap> simGraph = graph
              .run(new DefaultPreprocessing(DataDomain.NC, env));

          DataSet<Vertex<Long, ObjectMap>> representatives = simGraph
              .run(new TypeGroupBy(env))
              .run(new SimSort(DataDomain.NC, simSortThreshold, env))
              .getVertices()
              .runOperation(new RepresentativeCreatorMultiMerge(DataDomain.NC));

//    representatives.map(new MapFunction<Vertex<Long,ObjectMap>, Tuple3<Long, Integer, Set<Long>>>() {
//      @Override
//      public Tuple3<Long, Integer, Set<Long>> map(Vertex<Long, ObjectMap> value) throws Exception {
//        return new Tuple3<>(value.getId(), value.getValue().getVerticesCount(), value.getValue().getVerticesList());
//      }
//    }).sortPartition(1, Order.DESCENDING)
//        .setParallelism(1)
//        .first(50)
//        .print();

//    representatives.first(20).print();

          DataSet<Vertex<Long, ObjectMap>> merged = representatives
              .runOperation(new MergeInitialization(DataDomain.NC))
              .runOperation(new MergeExecution(
                  DataDomain.NC,
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

          printQuality(dataset, mergeThreshold, simSortThreshold, merged, sourcesCount);
        }
      }

    }

    assertEquals(1, 1);
  }

  private LogicalGraph getGradoopGraph(String graphPath) {
    final String graphHeadFile  = graphPath.concat("graphHeads.json");
    final String vertexFile     = graphPath.concat("vertices.json");
    final String edgeFile       = graphPath.concat("edges.json");
//    final String outputDir      = args[3];

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
    JSONDataSource dataSource = new JSONDataSource(graphHeadFile, vertexFile, edgeFile, config);
    return dataSource.getLogicalGraph();
  }

  private Graph<Long, ObjectMap, NullValue> getInputGraph(LogicalGraph logicalGraph) {
    // get gelly vertices
    DataSet<Vertex<Long, ObjectMap>> vertices = logicalGraph
        .getVertices()
        .map(new GradoopToObjectMapVertexMapper());

    // get gelly edges
    DataSet<Edge<Long, NullValue>> edges = logicalGraph.getEdges()
        .leftOuterJoin(logicalGraph.getVertices())
        .where(new SourceId<>())
        .equalTo(new Id<>())
        .with(new GradoopToGellyEdgeJoinFunction(0))
        .leftOuterJoin(logicalGraph.getVertices())
        .where(new TargetId<>())
        .equalTo(new Id<>())
        .with(new GradoopToGellyEdgeJoinFunction(1))
        .map(new GradoopEdgeToGellyEdgeMapper());

    return Graph.fromDataSet(vertices, edges, env);
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
//        if (secondSide.isEmpty()) { // TODO check: needed to have comparable results to alieh
//          out.collect(new Tuple2<>(first, first));
//        }
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
