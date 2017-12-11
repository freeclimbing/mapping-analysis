package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.blocking.BlockingStrategy;

import java.util.List;

public class IncrementalClustering
    implements GraphAlgorithm<Long, ObjectMap, NullValue, DataSet<Vertex<Long, ObjectMap>>> {

  private IncrementalClusteringFunction function;

  public IncrementalClustering(IncrementalClusteringFunction function) {
    this.function = function;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(Graph<Long, ObjectMap, NullValue> graph)
      throws Exception {
    return graph.run(function);
  }

  /**
   * Used for building a IncrementalClustering instance.
   */
  public static final class IncrementalClusteringBuilder {
    private IncrementalClusteringStrategy clusteringStrategy;
    private ExecutionEnvironment env = null;
    private List<String> sources;
    private Vertex<Long, ObjectMap> existingClusters;
    private BlockingStrategy blockingStrategy = BlockingStrategy.STANDARD_BLOCKING;
    private String part;

    public IncrementalClusteringBuilder setStrategy(
        IncrementalClusteringStrategy strategy) {
      this.clusteringStrategy = strategy;

      return this;
    }

    /**
     * Set of data sources as string values.
     * @param sources set of string values
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setDataSources(
        List<String> sources) {
      this.sources = sources;

      return this;
    }

    /**
     * Specify initial set of clusters for incremental clustering
     * @param existingClusters set of clusters
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setExistingClusters(
        Vertex<Long, ObjectMap> existingClusters) {
      this.existingClusters = existingClusters;

      return this;
    }

    /**
     * Set blocking strategy for incremental clustering. if not set: Standard blocking
     * @param strategy blocking strategy
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setBlockingStrategy(
        BlockingStrategy strategy) {
      this.blockingStrategy = strategy;

      return this;
    }

    // used for split incremental
    public IncrementalClusteringBuilder setPart(String part) {
      this.part = part;

      return this;
    }

    /**
     * Set execution environment.
     * @param env execution environment
     * @return IncrementalClusteringBuilder
     */
    public IncrementalClusteringBuilder setEnvironment(ExecutionEnvironment env) {
      this.env = env;

      return this;
    }

    /**
     * Creates incremental clustering based on the configured parameters.
     * @return instance of incremental clustering
     */
    public IncrementalClustering build() {
      if (env != null) {
        if (clusteringStrategy == IncrementalClusteringStrategy.MINSIZE) {
          return new MinSizeIncClustering(sources, env);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.FIXED_SEQUENCE) {
          return new FixedIncrementalClustering(blockingStrategy, env); // basic test clusteringStrategy
        } else if (clusteringStrategy == IncrementalClusteringStrategy.BIG) {
          return new BigIncrementalClustering(blockingStrategy, env);
        } else if (clusteringStrategy == IncrementalClusteringStrategy.SPLIT_SETTING) {
          return new SplitIncrementalClustering(blockingStrategy, part, env);
        } else {
          throw new IllegalArgumentException("Unsupported clusteringStrategy: " + clusteringStrategy);
        }
      } else {
        throw new IllegalArgumentException("Execution environment null");
      }
    }
  }
}
