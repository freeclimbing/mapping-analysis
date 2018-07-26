package org.mappinganalysis.util;

import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;
import org.mappinganalysis.model.functions.clusterstrategies.ClusteringStep;
import org.mappinganalysis.model.functions.incremental.MatchStrategy;
import org.mappinganalysis.util.config.IncrementalConfig;

/**
 * Provide methods supporting the execution of flink jobs.
 */
public class ExecutionUtils {

  /**
   * Get the job name for incremental run using the config params.
   * @param config IncrementalConfig
   * @return job name containing short params
   */
  public static String setJobName(IncrementalConfig config) {
    String jobName = "Inc-";
    if (config.getDataDomain() == DataDomain.MUSIC) {
      jobName = jobName.concat("Music-");
    } else if (config.getDataDomain() == DataDomain.NC) {
      jobName = jobName.concat("Nc-");
    } else if (config.getDataDomain() == DataDomain.GEOGRAPHY) {
      jobName = jobName.concat("Geo-");
    }
    if (config.getMatchStrategy() == MatchStrategy.MAX_BOTH) {
      jobName = jobName.concat("Mb-");
    } else if (config.getMatchStrategy() == MatchStrategy.HUNGARIAN) {
      jobName = jobName.concat("Hu-");
    }
    if (config.getStep() == ClusteringStep.SOURCE_ADDITION) {
      jobName = jobName.concat("Sa-");
    } else if (config.getStep() == ClusteringStep.VERTEX_ADDITION) {
      jobName = jobName.concat("Va-");
    }
    if (config.getBlockingStrategy() == BlockingStrategy.STANDARD_BLOCKING) {
      jobName = jobName.concat("Sb");
    } else if (config.getBlockingStrategy() == BlockingStrategy.BLOCK_SPLIT) {
      jobName = jobName.concat("Bs");
    }

    jobName = jobName + config.getBlockingLength()
        + "-"
        + config.getMinResultSimilarity();

    return jobName;
  }
}
