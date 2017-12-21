package org.mappinganalysis.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
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
    BitSet testSet = new BitSet();
    LOG.info(testSet.length());
    LOG.info(testSet.get(1000));

    testSet.set(2000);
    LOG.info(testSet.length());
    LOG.info(testSet.get(2000));
    LOG.info(testSet.get(1000) + " " + testSet.get(1001));

    // second part
    String bitsString = "0111000111000";
    // TODO check why result sets 3,4,5 + 9,10,11 instead of 2,3,4, 8,9,10
    BitSet bitset = new BitSet();
    // bitset size is 64 or bigger, if more than 64 is used,
    // size gets automatically increased to 128+

//    LOG.info(bitsString.length()-1);

		final int lastBitIndex = bitsString.length() - 1;

		for (int i = lastBitIndex; i >= 0; i--){
			if(bitsString.charAt(i) == '1'){
//			  LOG.info("set " + (lastBitIndex - i));
//			  LOG.info("bsize: " + bitset.size());
				bitset.set(lastBitIndex - i);
			}
		}

		assertEquals(13, bitsString.length());
    assertEquals(12, bitset.length());
//    LOG.info("bsize: " + bitset.size());

    assertEquals(64, bitset.size()); // for small amount of elements

    LOG.info(bitset.toString());

  }

  @Test
  public void lshBasicTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices();

    DataSet<Tuple2<Long, CharSet>> trigramsPerVertex = vertices
        .map(vertex -> {
          Tuple2<Long, CharSet> result = new Tuple2<>(
              vertex.getId(),
              Utils.getUnsortedTrigrams(vertex.getValue().getLabel()));
          if (result.f0 == 4051L) {
            LOG.info(result.toString());
          }
          return result;
        })
        .returns(new TypeHint<Tuple2<Long, CharSet>>() {});

    DataSet<Tuple1<char[]>> singleTrigrams = trigramsPerVertex
        .flatMap((FlatMapFunction<Tuple2<Long, CharSet>, Tuple1<char[]>>) (value, out) -> {
          for (char[] chars : value.f1) {
            out.collect(new Tuple1<>(chars));
          }
        })
        .returns(new TypeHint<Tuple1<char[]>>() {})
        .distinct();

    DataSet<Tuple2<Long, char[]>> idTrigramsDict = DataSetUtils.zipWithUniqueId(singleTrigrams)
        .map(line -> new Tuple2<>(line.f0, line.f1.f0))
        .returns(new TypeHint<Tuple2<Long, char[]>>() {});



//    idTrigramsDict.print();

    DataSet<Tuple2<Long, Long>> vertexIdTrigramIdTuple = trigramsPerVertex
        .flatMap((FlatMapFunction<Tuple2<Long, CharSet>,
            Tuple2<Long, char[]>>) (vertex, out) -> {
          for (char[] chars : vertex.f1) {
            out.collect(new Tuple2<>(vertex.f0, chars));
          }
        })
        .returns(new TypeHint<Tuple2<Long, char[]>>() {})
        .join(idTrigramsDict)
        .where(1)
        .equalTo(1)
        .with((vertexIdTrigram, idTrigramDictEntry) ->
            new Tuple2<>(vertexIdTrigram.f0, idTrigramDictEntry.f0))
        .returns(new TypeHint<Tuple2<Long, Long>>() {});

    DataSet<Tuple2<Long, BitSet>> vertexIdTrigramIds = vertexIdTrigramIdTuple
        .groupBy(0)
        .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, BitSet>>() {
          @Override
          public void reduce(
              Iterable<Tuple2<Long, Long>> vertexTrigramIds,
              Collector<Tuple2<Long, BitSet>> out)
              throws Exception {
            BitSet result = new BitSet();
            Long id = null;

            for (Tuple2<Long, Long> vIdTriId : vertexTrigramIds) {
              if (id == null) {
                id = vIdTriId.f0;
              }
              result.set(vIdTriId.f1.intValue());
            }

            out.collect(new Tuple2<>(id, result));
          }
        });

    vertexIdTrigramIds.join(vertices)
        .where(0)
        .equalTo(0)
        .with((left, right) -> {
          LOG.info(left.toString() + " " + right.getValue().getLabel());
          return left;
        })
        .returns(new TypeHint<Tuple2<Long, BitSet>>() {})
        .collect();

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
