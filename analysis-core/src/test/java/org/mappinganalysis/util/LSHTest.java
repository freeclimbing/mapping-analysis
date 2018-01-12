package org.mappinganalysis.util;

import com.google.common.base.CharMatcher;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.graph.utils.ConnectedComponentIdAdder;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.CharSet;
import org.mappinganalysis.model.functions.IncrementalClusteringTest;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.blocking.lsh.LshCandidateCreator;
import org.mappinganalysis.model.functions.blocking.lsh.structure.*;
import org.mappinganalysis.model.functions.blocking.lsh.trigrams.LongValueTrigramDictionaryCreator;
import org.mappinganalysis.model.functions.blocking.lsh.trigrams.TrigramPositionsToBitSetReducer;
import org.mappinganalysis.model.functions.blocking.lsh.trigrams.TrigramsPerVertexCreatorWithIdfOptimization;
import org.mappinganalysis.model.functions.blocking.lsh.utils.BitFrequencyCounter;
import org.mappinganalysis.model.functions.blocking.lsh.utils.CandidateMergeTripletCreator;
import org.mappinganalysis.model.functions.blocking.lsh.utils.VertexWithNewObjectMapFunction;
import org.mappinganalysis.model.functions.blocking.tfidf.TfIdfComputer;
import org.mappinganalysis.model.functions.merge.MergeGeoSimilarity;
import org.mappinganalysis.model.functions.merge.MergeGeoTupleCreator;
import org.mappinganalysis.model.functions.preprocessing.AddShadingTypeMapFunction;
import org.mappinganalysis.model.functions.simcomputation.SimilarityComputation;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.functions.LeftMinusRightSideJoinFunction;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;
import org.mappinganalysis.util.functions.keyselector.CcIdKeySelector;

import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LSHTest {
  private static final Logger LOG = Logger.getLogger(LSHTest.class);
  private static ExecutionEnvironment env = TestBase.setupLocalEnvironment();

  private final static String[] STOP_WORDS = {"test"};
//      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
//      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
//      "to", "not", "be", "with", "you", "have", "as", "can", "me", "my", "la",
//      "de", "no", "unknown"
//  };

  @Test
  public void keyPositionTest() throws Exception {
    final Integer[][] lshKeyPositions = HashFamilyGroup.selectRandomPositions(
        30,
        30,
        30000
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
  public void getUnsortedTrigramsTest() throws Exception {
    String test = "Petra";
    CharSet unsortedTrigrams = Utils.getUnsortedTrigrams(test);

    LOG.info(unsortedTrigrams);
    assertEquals(unsortedTrigrams.size(), 3);
  }

  @Test
  public void idfCheckWeightTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    DataSet<Tuple2<Long, String>> idLabelTuples = new JSONDataSource(
        graphPath, true, env)
        .getVertices()
        .map(vertex -> {
          String label = CharMatcher.WHITESPACE.trimAndCollapseFrom(
              vertex.getValue()
                  .getLabel()
                  .toLowerCase()
                  .replaceAll("[\\p{Punct}]", " "),
              ' ');
          return new Tuple2<>(vertex.getId(), label);
        })
        .returns(new TypeHint<Tuple2<Long, String>>() {});

    DataSet<Tuple2<String, Double>> idfValues = idLabelTuples
        .runOperation(new TfIdfComputer(STOP_WORDS))
        .sortPartition(1, Order.ASCENDING)
        .setParallelism(1);

    assertEquals(1.568201724066995D,
        idfValues.first(1).collect().iterator().next().f1,
        0.001);
  }

  @Test
  public void testCalculateLshKeys(){
    final Integer[][] lshKeyPositions = HashFamilyGroup.selectRandomPositions(
        15,
        15,
        50);

    HashFamilyGroup hashFamilyGroup = HashFamilyGroup.fromPositions(lshKeyPositions);

    BitSet result = new BitSet();
    result.set(10);
    result.set(23);
    result.set(42);
    result.set(1);
    BloomFilter bloomFilter = new BloomFilter(result);

    int keyCount = hashFamilyGroup.getNumberOfHashFamilies();
    LshKey[] keys = new LshKey[keyCount];

    List<List<Boolean>> hashValues = hashFamilyGroup
        .calculateHashes(bloomFilter.getBitset());

    for (int i = 0; i < hashValues.size(); i++){
      keys[i] = Lsh.calculateKey(hashValues.get(i), i);
    }

    for (LshKey key : keys) {
      LOG.info(key.getId() + " " + key.getBitset().toString());
    }
  }

  @Test
  public void labelCreationTest() throws Exception {
    String label = "Aix-en-Provence San Jose, (Calif)";
    label = label.toLowerCase()
        .replaceAll("[\\p{Punct}]", " ");
    label = CharMatcher.WHITESPACE.trimAndCollapseFrom(label, ' ');

    assertTrue(label.equals("aix en provence san jose calif"));
  }

  @Test
  public void tfIdfLshTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices();

    vertices = vertices
        .filter(new SourceFilterFunction(Constants.GN_NS))
        .union(vertices
            .filter(new SourceFilterFunction(Constants.NYT_NS)));

    DataSet<Tuple2<Long, Long>> candidateIds = vertices
        .runOperation(new LshCandidateCreator(true));


    SimilarityComputation<MergeGeoTriplet,
        MergeGeoTriplet> similarityComputation
        = new SimilarityComputation
        .SimilarityComputationBuilder<MergeGeoTriplet,
        MergeGeoTriplet>()
        .setSimilarityFunction(new MergeGeoSimilarity()) // TODO check sim function
        .setStrategy(SimilarityStrategy.MERGE)
        .setThreshold(0.3)
        .build();

    DataSet<MergeGeoTuple> geoTuples = vertices
        .map(new AddShadingTypeMapFunction())
        .map(new MergeGeoTupleCreator(BlockingStrategy.NO_BLOCKING));

    DataSet<MergeGeoTriplet> mergeGeoTriplets = candidateIds
//        .first(20)
        .map(candidate -> new MergeGeoTriplet(candidate.f0, candidate.f1))
        .returns(new TypeHint<MergeGeoTriplet>() {
        })
        .join(geoTuples)
        .where(0)
        .equalTo(0)
        .with(new CandidateMergeTripletCreator(0))
        .join(geoTuples)
        .where(1)
        .equalTo(0)
        .with(new CandidateMergeTripletCreator(1))
//        .filter(x -> x.getTrgId() == 301L || x.getSrcId() == 301L)
//        .returns(new TypeHint<MergeGeoTriplet>() {})
        .runOperation(similarityComputation);

    DataSet<Edge<Long, NullValue>> edges = mergeGeoTriplets
        .map(candidate -> new Edge<>(candidate.f0, candidate.f1, NullValue.getInstance()))
        .returns(new TypeHint<Edge<Long, NullValue>>() {});

    Graph<Long, ObjectMap, NullValue> graph = Graph.fromDataSet(edges, env)
        .mapVertices(new VertexWithNewObjectMapFunction())
        .run(new ConnectedComponentIdAdder<>(env));

//    graph.getVertices()
//        .groupBy(new CcIdKeySelector())
//        .reduceGroup(new HungarianAlgorithmReduceFunction())

    graph.getVertices()
//        .filter(value1 -> value1.getValue().getCcId() == 682L )//|| value1.getValue().getCcId() == 1115L)
        .join(vertices)
        .where(0)
        .equalTo(0)
        .with((JoinFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>)
            (first, second) -> second)
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {})
        .print();

    // LOGGING ONLY
    DataSet<Vertex<Long, ObjectMap>> logResult = graph.getVertices()
        .map(vertex -> {
          vertex.getValue().put("count", 1);
          return vertex;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {
        })
        .groupBy(new CcIdKeySelector())
        .reduce((value1, value2) -> {
//          if (value1.getValue().getCcId() == 682L || value1.getValue().getCcId() == 1115L) {
//            LOG.info(value1.toString() + " SECOND: " + value2.toString());
//          }
          value1.getValue().put("count", (int) value1.getValue().get("count")
              + (int) value2.getValue().get("count"));

          return value1;
        })
        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {
        });

    logResult.print();
    LOG.info(logResult.count());

    assertEquals(1504L, vertices.count());
  }

  @Test
  public void lshCompleteTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices();

    DataSet<Tuple2<Long, CharSet>> trigramsPerVertex = vertices
        .runOperation(new TrigramsPerVertexCreatorWithIdfOptimization(true));
    testTrigramsPerVertex(trigramsPerVertex);

    DataSet<Tuple2<Long, Long>> trigramIdsPerVertex = trigramsPerVertex
        .runOperation(new LongValueTrigramDictionaryCreator());
    assertEquals(21160, trigramIdsPerVertex.count());
//    LOG.info("count trigrams per vertex: " + trigramIdsPerVertex.count());

    DataSet<LinkageTuple> linkageTuples = trigramIdsPerVertex
        .groupBy(0)
        .reduceGroup(new TrigramPositionsToBitSetReducer());

    assertEquals(0, vertices.leftOuterJoin(linkageTuples)
        .where(0)
        .equalTo(0)
        .with(new LeftMinusRightSideJoinFunction<>()).count());
    assertEquals(vertices.count(), linkageTuples.count());

//    linkageTuples.print();

    // LOG: print lsh key id + bf + label of originating entity
//    verticesWithTrigramBitSet.join(vertices)
//        .where(0)
//        .equalTo(0)
//        .with((left, right) -> {
//          LOG.info("vertex id + bf: " + left.toString() + " label: " + right.getValue().getLabel());
//          return left;
//        })
//        .returns(new TypeHint<LinkageTuple>() {})
//        .collect();
  }

  private void testTrigramsPerVertex(DataSet<Tuple2<Long, CharSet>> trigramsPerVertex) throws Exception {
    DataSet<Tuple1<char[]>> singleTrigrams = trigramsPerVertex
        .flatMap((FlatMapFunction<Tuple2<Long, CharSet>, Tuple1<char[]>>) (value, out) -> {
          for (char[] chars : value.f1) {
            out.collect(new Tuple1<>(chars));
          }
        })
        .returns(new TypeHint<Tuple1<char[]>>() {});
    
    int trigramCount = (int) singleTrigrams.distinct().count();
    assertEquals(2737, trigramCount);

    // distinct trigrams and count of occurance
    DataSet<Tuple2<String, Integer>> distinctTrigramAndCount = singleTrigrams
        .groupBy(0)
        .reduceGroup(new GroupReduceFunction<Tuple1<char[]>, Tuple2<String, Integer>>() {
          @Override
          public void reduce(Iterable<Tuple1<char[]>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
            int i = 0;
            String first = null;
            for (Tuple1<char[]> chars : values) {
              if (first == null) {
                first = new String(chars.f0);
              }
              i++;
            }

            out.collect(new Tuple2<>(first, i));
          }
        })
        .sortPartition(1, Order.DESCENDING)
        .setParallelism(1)
        .first(3);

    for (Tuple2<String, Integer> tuple2 : distinctTrigramAndCount.collect()) {
      if (tuple2.f0.equals("ill")) {
        assertEquals(94, tuple2.f1.intValue());
      } else if (tuple2.f0.equals("lan")) {
        assertEquals(91, tuple2.f1.intValue());
      } else if (tuple2.f0.equals("ort")) {
        assertEquals(87, tuple2.f1.intValue());
      } else {
        assertTrue(false);
      }
    }
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
  }

  @Test
  public void lshBasicTest() throws Exception {
    String graphPath = IncrementalClusteringTest.class
        .getResource("/data/geography").getFile();

    DataSet<Vertex<Long, ObjectMap>> vertices =
        new JSONDataSource(graphPath, true, env)
            .getVertices();

//    vertices = vertices
//        .flatMap((FlatMapFunction<Vertex<Long, ObjectMap>, Vertex<Long, ObjectMap>>) (vertex, out) -> {
//          for (int i = 0; i < 10; i++) {
//            out.collect(new Vertex<>((vertex.getId() + 10000*i), vertex.getValue()));
//          }
//        })
//        .returns(new TypeHint<Vertex<Long, ObjectMap>>() {});

    LOG.info("vertices: " + vertices.count());

    /*
      Some working configs (small geo):
      3 entities Luang Brabang 2x + 1x Leipzig: 50 50 10000
      even entities (~2k): 50 50 20000
      all entities: 50 50 30k
     */
    int valueRangeLsh = 3200;
    final Integer[][] lshKeyPositions = HashFamilyGroup.selectRandomPositions(
        20,
        20,
        valueRangeLsh);

//    // TODO get frequent bit positions, min value is default setting
    final BitFrequencyCounter bfc = new BitFrequencyCounter(
        valueRangeLsh,  10);

//		DataSet<Integer> nonFrequentBitPositions = bfc.getNonFrequentBitPositions(verticesWithTrigramBitSet);
//// env.fromCollection(Collections.singletonList(Integer.MIN_VALUE));
//
////    nonFrequentBitPositions.print();
//
//		DataSet<Tuple2<LshKey, LinkageTupleWithLshKeys>> keyBloomFilterPairs =
//        verticesWithTrigramBitSet
//            .flatMap(new BloomFilterLshBlocker(lshKeyPositions))
//            .withBroadcastSet(nonFrequentBitPositions, "infrequentBits");
//
//    DataSet<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> keysWithCandidatePair =
//        keyBloomFilterPairs
//            .groupBy("f0.id", "f0.bits")
//            .reduceGroup(new BlockReducer());
//
//    LOG.info(keysWithCandidatePair.count());

//    DataSet<Tuple2<Long, Long>> candidateIds = keysWithCandidatePair
//        .map(pair -> {
//          Long left = pair.f1.getCandidateOne().getId();
//          Long right = pair.f1.getCandidateTwo().getId();
//          if (left < right) {
//            return new Tuple2<>(left, right);
//          } else {
//            return new Tuple2<>(right, left);
//          }
//        })
//        .returns(new TypeHint<Tuple2<Long, Long>>() {})
//        .distinct();

    assertEquals(749L, 749L);
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
}
