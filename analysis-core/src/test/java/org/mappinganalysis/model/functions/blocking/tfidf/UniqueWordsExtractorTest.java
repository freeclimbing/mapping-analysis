package org.mappinganalysis.model.functions.blocking.tfidf;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.merge.MergeMusicTupleCreator;
import org.mappinganalysis.util.Utils;

import java.util.HashMap;

/**
 * idf test
 */
public class UniqueWordsExtractorTest {
  private static final Logger LOG = Logger.getLogger(UniqueWordsExtractorTest.class);
  private static ExecutionEnvironment env;
  private final static String[] STOP_WORDS = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can", "me", "my", "la",
      // old: "love", "de", "no", "best", "music", "live", "hits", "from", "collection", "your", "unknown", "volume"
      "de", "no", "unknown"
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
  @Deprecated
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

    DataSet<MergeMusicTuple> realMusicTuples = vertices
        .map(new MapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>() {
          @Override
          public Vertex<Long, ObjectMap> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            String dataSource = vertex.getValue().getDataSource();
            vertex.getValue().setClusterDataSources(Sets.newHashSet(dataSource));
            vertex.getValue().setClusterVertices(Sets.newHashSet(vertex.getId()));

            return vertex;
          }
        })
        .map(new MergeMusicTupleCreator(BlockingStrategy.STANDARD_BLOCKING));
//            BlockingStrategy.IDF_BLOCKING));

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

    DataSet<Tuple2<Long, String>> idLabelTuples = realMusicTuples
        .map(new PrepareInputMapper());

    DataSet<Tuple2<String, Double>> idfValues = idLabelTuples
        .runOperation(new TfIdfComputer(STOP_WORDS));

    DataSet<Tuple2<Long, String>> idfDict = DataSetUtils
        .zipWithUniqueId(idfValues.map(tuple -> tuple.f0)
            .returns(new TypeHint<String>() {}));

    DataSet<Tuple2<Long, Long>> idfExtracted = idLabelTuples
        .flatMap(new HighIDFValueFlatMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf")
        .withBroadcastSet(idfDict, "idfDict");
//    idfExtracted.print();

    DataSet<Edge<Long, Integer>> idfSupportEdges = idfExtracted
        .groupBy(1) // idf string
        .reduceGroup(new IdfBasedEdgeCreator())
        .groupBy(0, 1)
        .sum(2)
        .filter(new SupportFilterFunction(2));

//    idfSupportEdges.print();

    DataSet<Edge<Long, ObjectMap>> edgeLabels = idfSupportEdges.join(idLabelTuples)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Edge<Long, Integer>, Tuple2<Long, String>, Edge<Long, ObjectMap>>() {
          @Override
          public void join(Edge<Long, Integer> first,
                           Tuple2<Long, String> second,
                           Collector<Edge<Long, ObjectMap>> out) throws Exception {
//            System.out.println("edge: " + first.toString());
//            System.out.println("tuple: " + second.toString());
            ObjectMap result = new ObjectMap();
            result.put("left", second.f1);

//            System.out.println("result: " + new Edge<>(first.f0, first.f1, result).toString());
            out.collect(new Edge<>(first.f0, first.f1, result));
          }
        })
        .join(idLabelTuples)
        .where(1)
        .equalTo(0)
        .with(new FlatJoinFunction<Edge<Long, ObjectMap>, Tuple2<Long, String>, Edge<Long, ObjectMap>>() {
          @Override
          public void join(Edge<Long, ObjectMap> first, Tuple2<Long, String> second, Collector<Edge<Long, ObjectMap>> out) throws Exception {
//            System.out.println("edge: " + first.toString());
//            System.out.println("tuple: " + second.toString());
            first.getValue().put("right", second.f1);

//            System.out.println("result: " + first.toString());
            out.collect(first);
          }
        });

//    edgeLabels.print();

    DataSet<Vertex<Long, Long>> ccVertices = createCcBlockingKeyVertices(idfSupportEdges);

    DataSet<MergeMusicTuple> unmatchedTuples = ccVertices.rightOuterJoin(realMusicTuples)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<Vertex<Long, Long>, MergeMusicTuple, MergeMusicTuple>() {
          @Override
          public void join(Vertex<Long, Long> first, MergeMusicTuple second,
                           Collector<MergeMusicTuple> out) throws Exception {
            if (first == null) {
              out.collect(second);
            }
          }
        });
//    unmatchedTuples.print();

    long unmatchedCount = unmatchedTuples.count();
    System.out.println("unmatched: " + unmatchedCount);

    DataSet<Tuple3<Long, Long, Integer>> vertexCcOne = vertices
        .map(new MapFunction<Vertex<Long, ObjectMap>, Tuple3<Long, Long, Integer>>() {
          @Override
          public Tuple3<Long, Long, Integer> map(Vertex<Long, ObjectMap> vertex) throws Exception {
            return new Tuple3<>(vertex.f0, vertex.getValue().getCcId(), 1);
          }
        })
        .groupBy(1)
        .sum(2)
        .filter(new FilterFunction<Tuple3<Long, Long, Integer>>() {
          @Override
          public boolean filter(Tuple3<Long, Long, Integer> value) throws Exception {
            return value.f2 == 1;
          }
        });

    System.out.println("vertices with cc size 1: " + vertexCcOne.count());

    DataSet<MergeMusicTuple> unmatchedInputAntiJoin = unmatchedTuples.leftOuterJoin(vertexCcOne)
        .where(0)
        .equalTo(0)
        .with(new FlatJoinFunction<MergeMusicTuple, Tuple3<Long, Long, Integer>, MergeMusicTuple>() {
          @Override
          public void join(MergeMusicTuple first, Tuple3<Long, Long, Integer> second, Collector<MergeMusicTuple> out) throws Exception {
            if (second == null) {
              out.collect(first);
            }
          }
        });

    DataSet<Edge<Long, Integer>> idfSupportEdgesSecond = computeForUnmatchedAnti(unmatchedInputAntiJoin);
    DataSet<Vertex<Long, Long>> ccVerticesSecond = createCcBlockingKeyVertices(idfSupportEdgesSecond);
    DataSet<Tuple2<Long, Long>> summedCcTuplesSecond = ccVerticesSecond
        .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
            return new Tuple2<>(vertex.getValue(), 1L);
          }
        })
        .groupBy(0)
        .sum(1);

    System.out.println("unmatched which are not in cc one input group: " + unmatchedInputAntiJoin.count());

    DataSet<MergeMusicTuple> unmatchedInputJoin = unmatchedTuples.join(vertexCcOne)
        .where(0)
        .equalTo(0)
        .with(new JoinFunction<MergeMusicTuple, Tuple3<Long, Long, Integer>, MergeMusicTuple>() {
          @Override
          public MergeMusicTuple join(MergeMusicTuple first, Tuple3<Long, Long, Integer> second) throws Exception {
            return first;
          }
        });

    System.out.println("unmatched input cc one value join: " + unmatchedInputJoin.count());


    DataSet<Tuple2<Long, Long>> summedCcTuples = ccVertices
        .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
          @Override
          public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
            return new Tuple2<>(vertex.getValue(), 1L);
          }
        })
        .groupBy(0)
        .sum(1);

//    idfValues.collect()
//        .stream()
//        .sorted((left, right) -> Double.compare(right.f1, left.f1))
//        .forEach(System.out::println);

    summedCcTuplesSecond.collect()
        .stream()
        .sorted((left, right) -> Long.compare(right.f1, left.f1))
        .forEach(System.out::println);

    summedCcTuplesSecond.sum(1).print();

    unmatchedInputAntiJoin.print();

    // old code removed, rewrite, if needed

//    LOG.info("all: " + idfSupportEdges.count());
//
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

//    DataSet<Edge<Long, Integer>> twoPlusEdges = idfSupportEdges
//        .filter(new FilterFunction<Edge<Long, Integer>>() {
//          @Override
//          public boolean filter(Edge<Long, Integer> value) throws Exception {
//            return value.f2 > 1;
//          }
//        });
//
//    LOG.info("two plus: " + twoPlusEdges.count());

//    DataSet<Edge<Long, NullValue>> ccEdges = vertices
//        .runOperation(new EdgeComputationVertexCcSet(new CcIdKeySelector()));
//
//    LOG.info("cc edges: " + ccEdges.count());


//    idfValues.collect()
//        .stream()
//        .sorted((left, right) -> Double.compare(right.f1, left.f1))
//        .forEach(System.out::println);

  }

  private DataSet<Edge<Long, Integer>> computeForUnmatchedAnti(DataSet<MergeMusicTuple> input) {
    DataSet<Tuple2<Long, String>> mmTuples = input
        .map(new PrepareInputMapper());

    DataSet<Tuple2<String, Double>> idfValues = mmTuples
        .runOperation(new TfIdfComputer(STOP_WORDS));

    DataSet<Tuple2<Long, String>> idfDict = DataSetUtils
        .zipWithUniqueId(idfValues.map(tuple -> tuple.f0)
            .returns(new TypeHint<String>() {}));

    DataSet<Tuple2<Long, Long>> idfExtracted = mmTuples
        .flatMap(new HighIDFValueFlatMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf")
        .withBroadcastSet(idfDict, "idfDict");
//    idfExtracted.print();

    DataSet<Edge<Long, Integer>> idfSupportEdges = idfExtracted
        .groupBy(1) // idf string
        .reduceGroup(new IdfBasedEdgeCreator())
        .groupBy(0, 1)
        .sum(2)
        .filter(new SupportFilterFunction(1));

    return idfSupportEdges;
  }

  @Deprecated
  private DataSet<Vertex<Long, Long>> createCcBlockingKeyVertices(DataSet<Edge<Long, Integer>> idfSupportEdges) {
    DataSet<Edge<Long, NullValue>> edges = idfSupportEdges
        .map(new MapFunction<Edge<Long, Integer>, Edge<Long, NullValue>>() {
          @Override
          public Edge<Long, NullValue> map(Edge<Long, Integer> value) throws Exception {
            return new Edge<>(value.getSource(), value.getTarget(), NullValue.getInstance());
          }
        });

    DataSet<Vertex<Long, Long>> vertices = edges
        .flatMap(new FlatMapFunction<Edge<Long, NullValue>, Vertex<Long, Long>>() {
          @Override
          public void flatMap(Edge<Long, NullValue> value, Collector<Vertex<Long, Long>> out) throws Exception {
            out.collect(new Vertex<>(value.getSource(), value.getSource()));
            out.collect(new Vertex<>(value.getTarget(), value.getTarget()));
          }
        }).distinct();

    Graph<Long, Long, NullValue> ccGraph = Graph.fromDataSet(vertices, edges, env);

    DataSet<Vertex<Long, Long>> ccVertices = null;
    try {
      ccVertices = ccGraph.run(new GSAConnectedComponents<>(Integer.MAX_VALUE));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ccVertices;
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
            return new Tuple1<>(Utils.createSimpleArtistTitleAlbum(value.getValue()));
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