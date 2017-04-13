package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.junit.Test;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.preprocessing.IsolatedEdgeRemover;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by markus on 4/13/17.
 */
public class MusicSimilarityFunctionTest {
  private static ExecutionEnvironment env;

  @Test
  public void testMusicSimilarityFunction() throws Exception {
    env = setupLocalEnvironment();

    final String TITLE_ONE = " Daniel Balavoine - L'enfant aux yeux d'Italie";
    final String TITLE_TWO = "L'enfant aux yeux d'Italie - De vous à elle en passant par moi";
    final String ARTIST_ONE = "--";
    final String ARTIST_TWO = "Daniel Balavoine";
    final String ALBUM_ONE = "De vous à elle en passant par moi";
    final String ALBUM_TWO = "--";
    final String LANGUAGE = "french";

    ObjectMap omOne = new ObjectMap();
    omOne.setLabel(TITLE_ONE);
    omOne.setArtist(ARTIST_ONE);
    omOne.setAlbum(ALBUM_ONE);
    omOne.setLanguage(LANGUAGE);

    ObjectMap omTwo = new ObjectMap();
    omTwo.setLabel(TITLE_TWO);
    omTwo.setArtist(ARTIST_TWO);
    omTwo.setAlbum(ALBUM_TWO);
    omTwo.setLanguage(LANGUAGE);

    List<Vertex<Long, ObjectMap>> vertexInput = Lists.newArrayList();
    vertexInput.add(new Vertex<>(1L, omOne));
    vertexInput.add(new Vertex<>(2L, omTwo));
    List<Edge<Long, NullValue>> edgeInput = Lists.newArrayList();
    edgeInput.add(new Edge<>(1L, 2L, NullValue.getInstance()));

    Graph<Long, ObjectMap, NullValue> graph = Graph.fromCollection(vertexInput, edgeInput, env);

    Graph<Long, ObjectMap, ObjectMap> run = graph
        .run(new BasicEdgeSimilarityComputation(Constants.MUSIC, env));

    run.getEdges()
        .print();


    StringMetric metric = Utils.getTrigramMetricAndSimplifyStrings();

    String[] splitTitleOne = TITLE_ONE.split("[-:]");
    ArrayList<String> one = Lists.newArrayList();
    Collections.addAll(one, splitTitleOne);
    one.add(ALBUM_ONE);

    String[] splitTitleTwo = TITLE_TWO.split("[-:]");
    ArrayList<String> two = Lists.newArrayList();
    Collections.addAll(two, splitTitleTwo);
    two.add(ARTIST_TWO);

    System.out.println(one);

    System.out.println(two);




//    if (!TITLE_ONE.equals(Constants.NO_LABEL_FOUND) && !trgLabel.equals(Constants.NO_LABEL_FOUND)) {
//      double similarity = metric.compare(srcLabel.toLowerCase().trim(), trgLabel.toLowerCase().trim());
//      BigDecimal tmpResult = new BigDecimal(similarity);
//      similarity = tmpResult.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
  }

  public static ExecutionEnvironment setupLocalEnvironment() {
    Configuration conf = new Configuration();
    conf.setInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY, 16384);
    env = new LocalEnvironment(conf);
    env.setParallelism(Runtime.getRuntime().availableProcessors());
    env.getConfig().disableSysoutLogging();

    return env;
  }
}