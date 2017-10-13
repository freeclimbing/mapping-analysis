package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.merge.MinThresholdFilterFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;

import java.math.BigDecimal;

public abstract class SimilarityComputation<T, O>
    implements CustomUnaryOperation<T, O> {
  private static final Logger LOG = Logger.getLogger(SimilarityComputation.class);

  private final Double threshold;
  private SimilarityFunction<T, O> function;
  private DataSet<T> inputData;
  private SimilarityStrategy strategy;

  public SimilarityComputation(SimilarityFunction<T, O> function,
                               SimilarityStrategy strategy,
                               Double threshold) {
    this.function = function;
    this.strategy = strategy;
    this.threshold = threshold;
  }

  @Override
  public void setInput(DataSet<T> inputData) {
    this.inputData = inputData;
  }

  /**
   * Execution of previously defined operators and aggregations to compute similarities.
   */
  @Override
  public DataSet<O> createResult() {
    if (inputData == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}

    if (strategy == SimilarityStrategy.MERGE) {
      return inputData
          .map(function)
          .filter(new MinThresholdFilterFunction<>(threshold));
    } else if (strategy == SimilarityStrategy.EDGE_SIM) {
      return inputData
          .map(function);
    } else if (strategy == SimilarityStrategy.MUSIC) { // customize?
      return inputData
          .map(function);
    } else {
      throw new IllegalArgumentException("Unsupported strategy: " + strategy);
    }
  }


  /**
   * Used for building the similarity computation operator instance.
   * @param <T> data type merge triple (working) or normal triple (to be implemented)
   */
  public static final class SimilarityComputationBuilder<T, O> {

    private SimilarityFunction<T, O> function;
    private AggregationMode<T> mode = null;
    private SimilarityStrategy strategy;
    private double threshold;

    public SimilarityComputationBuilder<T, O> setStrategy(SimilarityStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public SimilarityComputationBuilder<T, O> setSimilarityFunction(SimilarityFunction<T, O> function) {
      this.function = function;
      return this;
    }

    @Deprecated
    public SimilarityComputationBuilder<T, O> setAggregationMode(AggregationMode<T> mode) {
      this.mode = mode;
      return this;
    }

    /**
     * Set minimum threshold for similarity
     */
    public SimilarityComputationBuilder<T, O> setThreshold(double threshold) {
      this.threshold = threshold;
      return this;
    }

    /**
     * Creates similarity computation operator based on the configured parameters.
     * @return similarity computation operator
     */
    public SimilarityComputation<T, O> build() {
      // return different implementation for mergetriplet and normal triple
      if (strategy == SimilarityStrategy.MERGE) {
        return new MergeSimilarityComputation<>(function, strategy, threshold);
      } else if (strategy == SimilarityStrategy.EDGE_SIM) {
        return new EdgeSimilarityComputation<>(function, strategy, threshold);
      } else if (strategy == SimilarityStrategy.MUSIC) {
        return new MusicSimilarityComputation<>(function, strategy, threshold);
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    }

  }

  /**
   * Compose similarity values based on weights for each of the properties, missing values are counted as zero.
   *
   * create a similar function to MeanAggregationFunction for this one, if needed at all
   * Move to AggregationMode class
   * @param values property map
   * @return aggregated similarity value
   */
  @Deprecated
  public static double getWeightedAggSim(ObjectMap values) {
    double trigramWeight = 0.45;
    double typeWeight = 0.25;
    double geoWeight = 0.3;
    double aggregatedSim;
    if (values.containsKey(Constants.SIM_LABEL)) {
      aggregatedSim = trigramWeight * (double) values.get(Constants.SIM_LABEL);
    } else {
      aggregatedSim = 0;
    }
    if (values.containsKey(Constants.SIM_TYPE)) {
      aggregatedSim += typeWeight * (double) values.get(Constants.SIM_TYPE);
    }
    if (values.containsKey(Constants.SIM_DISTANCE)) {
      double distanceSim = (double) values.get(Constants.SIM_DISTANCE);
      if (Doubles.compare(distanceSim, -1) > 0) {
        aggregatedSim += geoWeight * distanceSim;
      }
    }

    BigDecimal result = new BigDecimal(aggregatedSim);
    result = result.setScale(10, BigDecimal.ROUND_HALF_UP);

    return result.doubleValue();
  }
}
