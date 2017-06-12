package org.mappinganalysis.benchmark.musicbrainz;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.MergeMusicTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.tfidf.HighIDFValueMapper;
import org.mappinganalysis.model.functions.blocking.tfidf.TfIdfComputer;
import org.mappinganalysis.model.functions.merge.MergeMusicTupleCreator;
import org.mappinganalysis.model.functions.merge.SourceCountRestrictionFilter;

/**
 * Created by markus on 6/12/17.
 */
public class SingleIdfComputation {
  private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

  private final static String[] STOP_WORDS = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can", "me", "my", "la", "all"
  };

  public static final String DECOMPOSITION_STEP = "musicbrainz-decomposition-representatives";

  public static String INPUT_PATH;



  public static void main(String[] args) throws Exception {
    INPUT_PATH = args[0];

    DataSet<Vertex<Long, ObjectMap>> mergedVertices =
        new JSONDataSource(INPUT_PATH, DECOMPOSITION_STEP, env)
            .getVertices();

    DataSet<MergeMusicTuple> clusters = mergedVertices
        .map(new MergeMusicTupleCreator());

    DataSet<MergeMusicTuple> preBlockingClusters = clusters
        .filter(new SourceCountRestrictionFilter<>(DataDomain.MUSIC, 5));

    DataSet<Tuple2<String, Double>> idfValues = preBlockingClusters
        .runOperation(new TfIdfComputer(STOP_WORDS));

    DataSet<Tuple2<Long, ObjectMap>> idfExtracted = preBlockingClusters
        .map(new HighIDFValueMapper(STOP_WORDS))
        .withBroadcastSet(idfValues, "idf");

    new JSONDataSink(INPUT_PATH, "idf")
        .writeTuples(idfExtracted);
    env.execute("SingleIdfComputation");
  }
}
