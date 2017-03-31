package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.primitives.Doubles;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.types.NullValue;
import org.apache.log4j.Logger;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.FullOuterJoinSimilarityValueFunction;
import org.mappinganalysis.model.functions.decomposition.simsort.TripletToEdgeMapFunction;
import org.mappinganalysis.model.functions.merge.MinThresholdFilterFunction;
import org.mappinganalysis.model.impl.SimilarityStrategy;
import org.mappinganalysis.util.Constants;

import java.math.BigDecimal;

public abstract class SimilarityComputation<T, O>
    implements CustomUnaryOperation<T, O> {
  private static final Logger LOG = Logger.getLogger(SimilarityComputation.class);

  private final Double threshold;
  private SimilarityFunction<T, O> function;
  private AggregationMode<T> mode;
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
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    }

  }

  /**
   * Compute similarities based on the existing vertex properties,
   * save aggregated similarity as edge property
   * @param graph input graph
   * @param matchCombination relevant: Utils.SIM_GEO_LABEL_STRATEGY or Utils.DEFAULT_VALUE
   * @return graph with edge similarities
   */
  @Deprecated
  public static Graph<Long, ObjectMap, ObjectMap> computeGraphEdgeSim(
      Graph<Long, ObjectMap, NullValue> graph,
      String matchCombination,
      ExecutionEnvironment env) {
    EdgeSimilarityFunction simFunction = new EdgeSimilarityFunction(
        matchCombination,
        Constants.MAXIMAL_GEO_DISTANCE); // todo agg mode?

    SimilarityComputation<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>> similarityComputation = new SimilarityComputation
        .SimilarityComputationBuilder<Triplet<Long, ObjectMap, NullValue>,
        Triplet<Long, ObjectMap, ObjectMap>>()
        .setSimilarityFunction(simFunction)
        .setStrategy(SimilarityStrategy.EDGE_SIM)
        .build();
    Constants.IGNORE_MISSING_PROPERTIES = true;

    DataSet<Edge<Long, ObjectMap>> edges = graph.getTriplets()
        .runOperation(similarityComputation)
        .map(new TripletToEdgeMapFunction())
        .map(new AggSimValueEdgeMapFunction(Constants.IGNORE_MISSING_PROPERTIES)); // old mean function

    return Graph.fromDataSet(graph.getVertices(), edges, env);
  }

  /**
   * Join several sets of triplets which are being produced within property similarity computation.
   * Edges where no similarity value is higher than the appropriate threshold are not in the result set.
   * @param tripletDataSet input data sets
   * @return joined dataset with all similarities in an ObjectMap
   */
  @SafeVarargs
  @Deprecated
  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> joinDifferentSimilarityValues(
      DataSet<Triplet<Long, ObjectMap, ObjectMap>>... tripletDataSet) {
    DataSet<Triplet<Long, ObjectMap, ObjectMap>> triplets = null;
    boolean isFirstSet = false;
    for (DataSet<Triplet<Long, ObjectMap, ObjectMap>> dataSet : tripletDataSet) {
      if (!isFirstSet) {
        triplets = dataSet;
        isFirstSet = true;
      } else {
        triplets = triplets
            .fullOuterJoin(dataSet)
            .where(0, 1)
            .equalTo(0, 1)
            .with(new FullOuterJoinSimilarityValueFunction());
      }
    }
    return triplets;
  }

  /**
   * Get a new triplet with an empty ObjectMap as edge value.
   * @param triplet triplet where edge value is NullValue
   * @return result triplet
   */
  @Deprecated
  public static Triplet<Long, ObjectMap, ObjectMap> initResultTriplet(Triplet<Long, ObjectMap, NullValue> triplet) {
    return new Triplet<>(
        triplet.getSrcVertex(),
        triplet.getTrgVertex(),
        new Edge<>(
            triplet.getSrcVertex().getId(),
            triplet.getTrgVertex().getId(),
            new ObjectMap()));
  }

  /**
   * Compose similarity values based on existence: if property is missing, its not considered at all.
   *
   * Move to AggregationMode class
   * @param values property map
   * @return mean similarity value
   */
  @Deprecated
  public static double getMeanSimilarity(ObjectMap values) {
    double aggregatedSim = 0;
    int propCount = 0;
    if (values.containsKey(Constants.SIM_TRIGRAM)) {
      ++propCount;
      aggregatedSim = (double) values.get(Constants.SIM_TRIGRAM);
    }
    if (values.containsKey(Constants.SIM_TYPE)) {
      ++propCount;
      aggregatedSim += (double) values.get(Constants.SIM_TYPE);
    }
    if (values.containsKey(Constants.SIM_DISTANCE)) {
      double distanceSim = (double) values.get(Constants.SIM_DISTANCE);
      if (Doubles.compare(distanceSim, -1) > 0) {
        aggregatedSim += distanceSim;
        ++propCount;
      }
    }

    BigDecimal result = new BigDecimal(aggregatedSim / propCount);
    result = result.setScale(10, BigDecimal.ROUND_HALF_UP);

    return result.doubleValue();
  }

  /**
   * Compose similarity values based on weights for each of the properties, missing values are counted as zero.
   *
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
    if (values.containsKey(Constants.SIM_TRIGRAM)) {
      aggregatedSim = trigramWeight * (double) values.get(Constants.SIM_TRIGRAM);
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
