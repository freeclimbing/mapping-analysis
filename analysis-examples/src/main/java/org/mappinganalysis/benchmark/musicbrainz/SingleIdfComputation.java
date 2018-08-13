package org.mappinganalysis.benchmark.musicbrainz;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.blocking.tfidf.PrepareInputMapper;
import org.mappinganalysis.model.functions.blocking.tfidf.SingleIdfHighMapper;
import org.mappinganalysis.model.functions.blocking.tfidf.TfIdfComputer;
import org.mappinganalysis.model.functions.blocking.tfidf.UniqueWordExtractor;
import org.mappinganalysis.model.functions.merge.MergeTupleCreator;
import org.mappinganalysis.model.functions.merge.SourceCountRestrictionFilter;

/**
 * Compute idf values for representative vertices (music dataset only)
 */
public class SingleIdfComputation {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private final static String[] STOP_WORDS = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can", "me", "my", "la", "all"
  };

  private static final String DECOMPOSITION_STEP = "musicbrainz-decomposition-representatives";
  private static String INPUT_PATH;
  private static String VERTEX_FILE_NAME;

  public static void main(String[] args) throws Exception {
    Preconditions.checkArgument(args.length == 2, "args[0]: input dir" +
        "args[1]: file name");
    INPUT_PATH = args[0];
    VERTEX_FILE_NAME = args[1];

    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(INPUT_PATH, DECOMPOSITION_STEP, env)
            .getVertices();

    DataSet<MergeTuple> clusters = mergedVertices
        .map(new MergeTupleCreator(BlockingStrategy.IDF_BLOCKING, DataDomain.MUSIC, 4));

    DataSet<MergeTuple> preBlockingClusters = clusters
        .filter(new SourceCountRestrictionFilter<>(DataDomain.MUSIC, 5));


    DataSet<Tuple2<Long, String>> preTuples = preBlockingClusters
        .map(new PrepareInputMapper());

    // added
    DataSet<Tuple3<String, String, Integer>> uniqueWords = preTuples
        .<Tuple1<String>>project(1)
        .flatMap(new UniqueWordExtractor(STOP_WORDS));

    DataSet<Tuple2<String, Integer>> wordFrequency = uniqueWords
        .map(new MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
          @Override
          public Tuple2<String, Integer> map(Tuple3<String, String, Integer> input) throws Exception {
            String uniqueWord = input.f1;
            Integer count = input.f2;
            return new Tuple2<>(uniqueWord, count);
          }
        })
        .groupBy(0)
        .sum(1);

    new JSONDataSink(INPUT_PATH, "word-frequency-aggr-".concat(VERTEX_FILE_NAME))
        .writeTuples(wordFrequency);
    // end

    DataSet<Tuple2<String, Double>> idfValues = preTuples
        .runOperation(new TfIdfComputer(STOP_WORDS));

    new JSONDataSink(INPUT_PATH, "all-options-idf-".concat(VERTEX_FILE_NAME))
        .writeTuples(idfValues);

    DataSet<Tuple2<Long, String>> idfExtracted = preBlockingClusters
        .flatMap(new SingleIdfHighMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf");

    new JSONDataSink(INPUT_PATH, "best-idf-".concat(VERTEX_FILE_NAME))
        .writeTuples(idfExtracted);
    env.execute("SingleIdfComputation");
  }
}
