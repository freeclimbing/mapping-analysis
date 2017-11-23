package org.mappinganalysis.model.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.mappinganalysis.model.ObjectMap;

import java.util.List;

public class IncrementalClustering
    implements GraphAlgorithm<Long, ObjectMap, ObjectMap, DataSet<Vertex<Long, ObjectMap>>> {

  private IncrementalClusteringFunction function;

  public IncrementalClustering(IncrementalClusteringFunction function) {
    this.function = function;
  }

  @Override
  public DataSet<Vertex<Long, ObjectMap>> run(Graph<Long, ObjectMap, ObjectMap> graph)
      throws Exception {
    return graph.run(function);
  }

  /**
   * Used for building a IncrementalClustering instance.
   */
  public static final class IncrementalClusteringBuilder {
    private IncrementalClusteringStrategy strategy;
    private ExecutionEnvironment env = null;
    private List<String> sources;
    private Vertex<Long, ObjectMap> existingClusters;

    public IncrementalClusteringBuilder setStrategy(
        IncrementalClusteringStrategy strategy) {
      this.strategy = strategy;
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
        if (strategy == IncrementalClusteringStrategy.MINSIZE) {
          return new MinSizeIncClustering(sources, env);
        } else if (strategy == IncrementalClusteringStrategy.FIXED) {
          return new FixedIncrementalClustering(env); // basic test strategy
        } else {
          throw new IllegalArgumentException("Unsupported strategy: " + strategy);
        }
      } else {
        throw new IllegalArgumentException("Execution environment null");
      }
    }
  }
}
