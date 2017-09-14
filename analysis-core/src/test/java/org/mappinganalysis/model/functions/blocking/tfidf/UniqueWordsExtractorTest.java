package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Vertex;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mappinganalysis.io.impl.csv.CSVDataSource;
import org.mappinganalysis.model.ObjectMap;

/**
 * idf test
 */
public class UniqueWordsExtractorTest {
  private static final Logger LOG = Logger.getLogger(UniqueWordsExtractorTest.class);
  private static ExecutionEnvironment env;
  private final static String[] STOP_WORDS = {
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is",
      "there", "it", "this", "that", "on", "was", "by", "of", "to", "in",
      "to", "not", "be", "with", "you", "have", "as", "can"
  };

  @Test
  public void testUniqueWordExtractor() throws Exception {
    env = setupLocalEnvironment();

    String path = UniqueWordsExtractorTest.class
        .getResource("/data/musicbrainz/")
        .getFile();
    final String vertexFileName = "musicbrainz-20000-A01.csv.dapo";
    DataSet<Vertex<Long, ObjectMap>> vertices = new CSVDataSource(path, vertexFileName, env)
        .getVertices();

    DataSet<Tuple2<String, Double>> idfValues = vertices.runOperation(new TfIdfComputer(STOP_WORDS));

    idfValues.collect()
        .stream()
        .sorted((left, right) -> Double.compare(right.f1, left.f1))
        .forEach(System.out::println);

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