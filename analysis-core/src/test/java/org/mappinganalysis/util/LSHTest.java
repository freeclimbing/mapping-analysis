package org.mappinganalysis.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CharSet;
import org.mappinganalysis.model.functions.IncrementalClusteringTest;
import org.mappinganalysis.model.functions.blocking.lsh.utils.HashFamilyGroup;

import java.util.Arrays;
import java.util.BitSet;

import static org.junit.Assert.assertEquals;

public class LSHTest {
  private static final Logger LOG = Logger.getLogger(LSHTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  @Test
  public void keyPositionTest() throws Exception {
    int lshKeyValueRange;
    lshKeyValueRange = 30000;

    final Integer[][] lshKeyPositions =
        HashFamilyGroup.selectRandomPositions(
            30,
            30,
            lshKeyValueRange
        );


    //TODO DONT PRINT LINES; BUT TEST
    for (Integer[] lshKeyPosition : lshKeyPositions) {
      String line = "";
      for (Integer integer : lshKeyPosition) {
        line = line.concat(integer.toString() + " ");
      }
      LOG.info(line);
    }
  }

  @Test
  public void bitSetTest() throws Exception {
    BitSet set = new BitSet();

    LOG.info(set.length());

    LOG.info(set.get(1000));

    set.set(2000);

    LOG.info(set.length());

    LOG.info(set.get(2000));
    LOG.info(set.get(1000) + " " + set.get(1001));


  }

  @Test
  public void lshBasicTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices();

    DataSet<Tuple2<Long, CharSet>> trigramsPerVertex = vertices
        .map(vertex -> new Tuple2<>(
            vertex.getId(),
            Utils.getUnsortedTrigrams(vertex.getValue().getLabel())))
        .returns(new TypeHint<Tuple2<Long, CharSet>>() {});

    DataSet<Tuple1<char[]>> singleTrigrams = trigramsPerVertex
        .flatMap((FlatMapFunction<Tuple2<Long, CharSet>, Tuple1<char[]>>) (value, out) -> {
          for (char[] chars : value.f1) {
            out.collect(new Tuple1<>(chars));
          }
        })
        .returns(new TypeHint<Tuple1<char[]>>() {});

    DataSet<Tuple2<Long, char[]>> idTrigrams = DataSetUtils.zipWithUniqueId(singleTrigrams)
        .map(line -> new Tuple2<>(line.f0, line.f1.f0))
        .returns(new TypeHint<Tuple2<Long, char[]>>() {});

    idTrigrams.print();

//    idTrigrams.flatMap((tuple, out) -> {
//      for (char c : tuple.f1) {
//        out.collect(tuple.f0, char);
//      }
//    })

    // size of vectors == number of different trigrams
//    int trigramCount = (int) singleTrigrams.distinct().count();
//    // number of sets == number of entities
//    int entityCount = (int) vertices.count();



    final Integer[][] lshKeyPositions = getLshKeyPositions(); // TODO 10/15/16 specify parameters

    // TODO get linkageTuples with bloomfilters

    // TODO get (frequent) bit positions
		DataSet<Integer> nonFrequentBitPositions = env
        .fromCollection(Arrays.asList(Integer.MIN_VALUE));

//		DataSet<Tuple2<LshKey, LinkageTupleWithLshKeys>> keyBloomFilterPairs =
//				calculateLshKeys(linkageTuples, lshKeyPositions, nonFrequentBitPositions);





//    // distinct trigrams and count of occurance
//    DataSet<Tuple2<String, Integer>> distinctTrigramAndCount = singleTrigrams
//        .groupBy(0)
//        .reduceGroup(new GroupReduceFunction<Tuple1<char[]>, Tuple2<String, Integer>>() {
//          @Override
//          public void reduce(Iterable<Tuple1<char[]>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
//            int i = 0;
//            String first = null;
//            for (Tuple1<char[]> chars : values) {
//              if (first == null) {
//                first = new String(chars.f0);
//              }
//              i++;
//            }
//
//            out.collect(new Tuple2<>(first, i));
//          }
//        })
//        .sortPartition(1, Order.DESCENDING)
//        .setParallelism(1);

//    // test that no trigram is hashed to 2x same integer
//    singleTrigrams.distinct()
//        .map(chars -> new Tuple2<char[], Integer>(
//            chars.f0,
//            Utils.getHash(new String(chars.f0)).intValue()))
//        .returns(new TypeHint<Tuple2<char[], Integer>>() {})
//        .groupBy(1)
//        .reduceGroup(new GroupReduceFunction<Tuple2<char[], Integer>, Tuple3<String, Integer, Integer>>() {
//          @Override
//          public void reduce(Iterable<Tuple2<char[], Integer>> values, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
//            int i = 0;
//            String label = null;
//            Integer hash = null;
//            for (Tuple2<char[], Integer> chars : values) {
//              hash = chars.f1;
//              if (label == null) {
//                label = new String(chars.f0);
//              }
//              i++;
//            }
//
//            out.collect(new Tuple3<>(label, hash, i));
//          }
//        })
//        .sortPartition(2, Order.ASCENDING)
//        .setParallelism(1)
//        .map(x->{
//          LOG.info(x.toString());
//          return x;
//        })
//        .returns(new TypeHint<Tuple3<String, Integer, Integer>>() {})
//        .collect();

    assertEquals(749L, 749L);
  }

  private Integer[][] getLshKeyPositions(){
    int lshKeyValueRange;
      lshKeyValueRange = 100;

    final Integer[][] lshKeyPositions =
        HashFamilyGroup.selectRandomPositions(
            10, 15,
            lshKeyValueRange
        );

    return lshKeyPositions;
  }

//  private DataSet<Tuple2<LshKey, LinkageTupleWithLshKeys>> calculateLshKeys(
//      DataSet<LinkageTuple> data,
//      Integer[][] lshKeyPositions,
//      DataSet<Integer> infrequentBits) throws IOException {
//    // build blocks (use LSH for this) of bloom filters, where matches are supposed
//
//    return data
//        .flatMap(new BloomFilterLshBlocker(lshKeyPositions))
//        .withBroadcastSet(infrequentBits, "infrequentBits");
//  }

//  public void setUp() throws Exception {
//    ParseData myParser = new ParseData("searchout/test_iphone_6_plus.json",false);
//    List<Item> itemList = myParser.readData();
//
//    lshIndex = new ItemLSHIndex(itemList);
//    lshIndex.createLSH(bucket);
//  }
//
//  public void findCandidateItemsTest() throws Exception {
//    List<Integer> candidates = lshIndex.findCandidateItems(0);
//    List<Integer> expected = Arrays.asList(1, 2);
//    assertEquals(expected, candidates);
//  }
}
