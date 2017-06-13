package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.merge.MergeMusicTupleCreator;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;
import java.util.HashSet;

/**
 * idf test
 */
public class UniqueWordsExtractorTest {
  private static final Logger LOG = Logger.getLogger(UniqueWordsExtractorTest.class);
  private static ExecutionEnvironment env;
  private final static String[] STOP_WORDS = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can", "me", "my", "la", "all"
  };

  @Test
  public void maxElementTest() throws Exception {
    ObjectMap map = new ObjectMap();
    HashMap<String, Double> check = Maps.newHashMap();
    check.put("foo", 2d);
    check.put("bar", 2d);

    map.addMinValueToResult(check);

    System.out.println(map.getIDFs().toString());
  }

  @Test
  public void testUniqueWordExtractor() throws Exception {
    env = setupLocalEnvironment();

    String path = UniqueWordsExtractorTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices();//;
//        .filter(new FilterFunction<Vertex<Long, ObjectMap>>() {
//          @Override
//          public boolean filter(Vertex<Long, ObjectMap> vertex) throws Exception {
//            return vertex.getValue().getCcId() < 10L;
//          }
//        });

//    vertices.print();



    DataSet<MergeMusicTuple> mmTuples = vertices
        .map(new MapFunction<Vertex<Long,ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            String dataSource = vertex.getValue().getDataSource();
            vertex.getValue().setClusterDataSources(Sets.newHashSet(dataSource));
            vertex.getValue().setClusterVertices(Sets.newHashSet(vertex.getId()));

            return vertex;
          }
        })
        .map(new MergeMusicTupleCreator());

    // 2SmoothSavageLife2
    // UniversalBeing

    // stats only?
//    DataSet<Tuple3<String, String, Integer>> uniqueWords = vertices
//        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple1<String>>() {
//          @Override
//          public Tuple1<String> map(Vertex<Long, ObjectMap> value) throws Exception {
//            return new Tuple1<>(Utils.createArtistTitleAlbum(value));
//          }
//        })
//        .flatMap(new UniqueWordExtractor(STOP_WORDS));
//    printStats(vertices, uniqueWords);

    DataSet<Tuple2<String, Double>> idfValues = mmTuples
        .runOperation(new TfIdfComputer(STOP_WORDS));

    DataSet<Tuple2<Long, ObjectMap>> idfExtracted = mmTuples
        .map(new HighIDFValueMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf");

//    idfExtracted.collect();

    DataSet<Edge<Long, Integer>> idfSupportEdges = idfExtracted
        // VertexIdfSingleValueExtractor in IdfBlockingOperation
        .flatMap(new FlatMapFunction<Tuple2<Long, ObjectMap>, Tuple2<Long, String>>() {
          @Override
          public void flatMap(Tuple2<Long, ObjectMap> tuple,
                              Collector<Tuple2<Long, String>> out) throws Exception {
            ObjectMap idfValues = tuple.f1;
//            HashMap<String, Double> idfs = tuple.f1;
            for (String idf : idfValues.keySet()) {
              out.collect(new Tuple2<>(tuple.f0, idf));
            }
          }
        })
        .groupBy(1)
        .reduceGroup(new GroupReduceFunction<Tuple2<Long, String>, Edge<Long, Integer>>() {
          @Override
          public void reduce(
              Iterable<Tuple2<Long, String>> values,
              Collector<Edge<Long, Integer>> out) throws Exception {
            HashSet<Tuple2<Long, String>> leftSide = Sets.newHashSet(values);
            HashSet<Tuple2<Long, String>> rightSide = Sets.newHashSet(leftSide);

            for (Tuple2<Long, String> leftTuple : leftSide) {
              rightSide.remove(leftTuple);
              for (Tuple2<Long, String> rightTuple : rightSide) {
                if (leftTuple.f0 < rightTuple.f0) {
                  out.collect(new Edge<>(leftTuple.f0, rightTuple.f0, 1));
                } else {
                  out.collect(new Edge<>(rightTuple.f0, leftTuple.f0, 1));
                }
              }
            }
          }
        })
        .groupBy(0, 1)
        .sum(2);

//    LOG.info("all: " + idfSupportEdges.count());
//
//    DataSet<Edge<Long, Integer>> fourPlusEdges = idfSupportEdges
//        .filter(new FilterFunction<Edge<Long, Integer>>() {
//      @Override
//      public boolean filter(Edge<Long, Integer> value) throws Exception {
//        return value.f2 > 3;
//      }
//    });
//
//    LOG.info("four plus: " + fourPlusEdges.count());
//
//    DataSet<Edge<Long, Integer>> threePlusEdges = idfSupportEdges
//        .filter(new FilterFunction<Edge<Long, Integer>>() {
//          @Override
//          public boolean filter(Edge<Long, Integer> value) throws Exception {
//            return value.f2 > 2;
//          }
//        });
//
//    LOG.info("three plus: " + threePlusEdges.count());

    DataSet<Edge<Long, Integer>> twoPlusEdges = idfSupportEdges
        .filter(new FilterFunction<Edge<Long, Integer>>() {
          @Override
          public boolean filter(Edge<Long, Integer> value) throws Exception {
            return value.f2 > 1;
          }
        });

    LOG.info("two plus: " + twoPlusEdges.count());

//    DataSet<Edge<Long, NullValue>> ccEdges = vertices
//        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector()));
//
//    LOG.info("cc edges: " + ccEdges.count());


//    idfValues.collect()
//        .stream()
//        .sorted((left, right) -> Double.compare(right.f1, left.f1))
//        .forEach(System.out::println);

  }

  private void printStats(DataSet<Vertex<Long, ObjectMap>> vertices, DataSet<Tuple3<String, String, Integer>> uniqueWords) throws Exception {
    System.out.println(uniqueWords.count());
    uniqueWords.groupBy(0).sum(2).filter(new FilterFunction<Tuple3<String, String, Integer>>() {
      @Override
      public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
        return value.f2 < 2;
      }
    }).print();

    System.out.println(vertices
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple1<String>>() {
          @Override
          public Tuple1<String> map(Vertex<Long, ObjectMap> value) throws Exception {
            return new Tuple1<>(Utils.createArtistTitleAlbum(value));
          }
        }).distinct().count());

    System.out.println(vertices.count());
    System.out.println(uniqueWords.distinct(0).count());

    /**
     * small size of artist title album
     */
//    vertices.map(new MapFunction<Vertex<Long,ObjectMap>, String>() {
//      @Override
//      public String map(Vertex<Long, ObjectMap> vertex) throws Exception {
//        String artistTitleAlbum = Utils.createArtistTitleAlbum(vertex);
//        if (artistTitleAlbum.length() <16) {
//          System.out.println(vertex.toString());
//        }
//        return artistTitleAlbum;
//      }
//    })
//        .filter(new FilterFunction<String>() {
//          @Override
//          public boolean filter(String value) throws Exception {
//            return value.length() < 16;
//          }
//        })
//        .print();
  }

  private static ExecutionEnvironment setupLocalEnvironment() {
    Configuration conf = new Configuration();
    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 16384);
    env = new LocalEnvironment(conf);
    env.setParallelism(Runtime.getRuntime().availableProcessors());
    env.getConfig().disableSysoutLogging();

    return env;
  }

}