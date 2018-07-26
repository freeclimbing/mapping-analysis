package org.mappinganalysis.integration;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;
import org.mappinganalysis.NcBaseTest;
import org.mappinganalysis.TestBase;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.io.impl.json.JSONDataSink;
import org.mappinganalysis.io.impl.json.JSONDataSource;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClustering;
import org.mappinganalysis.model.functions.clusterstrategies.IncrementalClusteringStrategy;
import org.mappinganalysis.model.functions.incremental.MatchStrategy;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.ExecutionUtils;
import org.mappinganalysis.util.QualityUtils;
import org.mappinganalysis.util.Utils;
import org.mappinganalysis.util.config.IncrementalConfig;
import org.mappinganalysis.util.functions.filter.SourceFilterFunction;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class IncrementalNcClusteringTest {
  private static final Logger LOG = Logger
      .getLogger(IncrementalNcClusteringTest.class);
  private static ExecutionEnvironment env = ExecutionEnvironment
      .getExecutionEnvironment();

  @Test
  public void stringSplitTest() throws Exception {
    String toSplit = "/user/saeedi/1000-ocp20/output/10987654321Inc-Nc-Mb-Sa-Bs-0.7/";

    if (toSplit.endsWith("/")) {
      toSplit = toSplit.substring(0, toSplit.length() - 1);
    }

    Iterator<String> split = Splitter.on('/').split(toSplit).iterator();

    String jobName = "";
    while (split.hasNext()) {
      jobName = split.next();
      System.out.println(jobName);
    }

    System.out.println("jobname: " + jobName);

    String newPath = toSplit.substring(0, toSplit.length() - jobName.length());

    System.out.println("newPath: " + newPath);

  }

  @Test
  public void initialNcTest() throws Exception {
    env = TestBase.setupLocalEnvironment();

    List<String> sourceList = Lists.newArrayList(//"/data/nc/10s1/",
//        "/data/nc/10s2/",
        "/data/nc/10s5/");
    for (String dataset : sourceList) {

      final String path = NcBaseTest.class.getResource(dataset).getFile();

      LogicalGraph logicalGraph = Utils
          .getGradoopGraph(path, env);
      Graph<Long, ObjectMap, NullValue> baseGraph = Utils
          .getInputGraph(logicalGraph, Constants.NC, env);

      IncrementalConfig config = new IncrementalConfig(DataDomain.NC, env);
      config.setBlockingStrategy(BlockingStrategy.BLOCK_SPLIT);
      config.setStrategy(IncrementalClusteringStrategy.MULTI);
      config.setMetric(Constants.COSINE_TRIGRAM);
      config.setStep(ClusteringStep.SOURCE_ADDITION);
      config.setMatchStrategy(MatchStrategy.MAX_BOTH);
      config.setMinResultSimilarity(0.6);
      config.setBlockingLength(4);

      LOG.info(config.toString());
      String jobName = ExecutionUtils.setJobName(config);

      boolean isFirst = true;
      boolean isSecond = true;
      Graph<Long, ObjectMap, NullValue> inputGraph = null;
      DataSet<Vertex<Long, ObjectMap>> newVertices;
      DataSet<Vertex<Long, ObjectMap>> clusters = null;

      for (String source : Constants.NC_SOURCES) {
        System.out.println("Adding source " + source);
        if (isFirst) {
          inputGraph = baseGraph
              .filterOnVertices(new SourceFilterFunction(source));
          jobName = source.replaceAll("[^\\d.]", "")
              .concat(jobName);
          isFirst = false;
        } else {
          if (isSecond) {
            jobName = source.replaceAll("[^\\d.]", "")
                .concat(jobName);
            isSecond = false;
          } else {
            inputGraph = new JSONDataSource(path, jobName, env)
                .getGraph(ObjectMap.class, NullValue.class);
            jobName = source.replaceAll("[^\\d.]", "")
                .concat(jobName);
          }

          newVertices = baseGraph.getVertices()
              .filter(new SourceFilterFunction(source));

          IncrementalClustering clustering = new IncrementalClustering
              .IncrementalClusteringBuilder(config)
              .setNewSource(source)
              .setMatchElements(newVertices)
              .build();

          clusters = inputGraph.run(clustering);

          List<Long> resultingVerticesList = Lists.newArrayList();
          Collection<Vertex<Long, ObjectMap>> representatives = Lists.newArrayList();
          clusters.output(new LocalCollectionOutputFormat<>(representatives));
          new JSONDataSink(path, jobName)
              .writeVertices(clusters);
          JobExecutionResult execResult = env.execute();

          QualityUtils.printExecPlusAccumulatorResults(execResult);

          for (Vertex<Long, ObjectMap> representative : representatives) {
//            if (representative.getValue().getVerticesList().contains(107858765L)) {
//              LOG.info(representative.toString());
//            }
            resultingVerticesList.addAll(representative.getValue().getVerticesList());
//            LOG.info(representative.toString());
          }

          LOG.info("all contained: " + resultingVerticesList.size());


          HashSet<Long> uniqueVerticesSet = Sets.newHashSet(resultingVerticesList);

          HashSet<Object> testSet = Sets.newHashSet();
          for (Long duplicatePotential : resultingVerticesList) {
            if (!testSet.add(duplicatePotential)) {
              System.out.println("duplicate id: " + duplicatePotential);
            }
          }

          assertEquals(resultingVerticesList.size(), uniqueVerticesSet.size());

        }
      }

      inputGraph = new JSONDataSource(path, jobName, env)
          .getGraph(ObjectMap.class, NullValue.class);

      QualityUtils.printNcQuality(inputGraph.getVertices(),
          config,
          path,
          "local",
          jobName);
    }
  }

}
