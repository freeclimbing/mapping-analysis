package org.mappinganalysis.model.functions.simcomputation;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CustomUnaryOperation;
import org.apache.flink.api.java.operators.MapOperator;
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

public abstract class SimilarityComputation<T>
    implements CustomUnaryOperation<T, T> {
  private static final Logger LOG = Logger.getLogger(SimilarityComputation.class);

  private final Double threshold;
  private SimilarityFunction<T> function;
  private AggregationMode<T> mode;
  private DataSet<T> inputData;
  private SimilarityStrategy strategy;

  public SimilarityComputation(SimilarityFunction<T> function,
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
  public DataSet<T> createResult() {
    if (inputData == null) {
			throw new IllegalStateException("The input data set has not been set.");
		}

    if (strategy == SimilarityStrategy.MERGE) {
      return inputData
          .map(function)
          .filter((FilterFunction<T>) new MinThresholdFilterFunction(threshold));
    } else {
      throw new IllegalArgumentException("Unsupported strategy: " + strategy);
    }
  }

  /**
   * Decide which similarities should be computed based on filter
   * @param triplets graph triplets
   * @param filter strategy: geo, label, type, [empty, combined] -> all 3 combined
   * @return triplets with sim values
   */
  public static DataSet<Triplet<Long, ObjectMap, ObjectMap>> computeSimilarities(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets, String filter) {
    switch (filter) {
      case Constants.SIM_GEO_LABEL_STRATEGY:
        return joinDifferentSimilarityValues(basicGeoSimilarity(triplets),
            basicTrigramSimilarity(triplets));
      case "label":
        return basicTrigramSimilarity(triplets);
      case "type":
        return basicTypeSimilarity(triplets);
      default:
        return joinDifferentSimilarityValues(basicGeoSimilarity(triplets),
            basicTrigramSimilarity(triplets),
            basicTypeSimilarity(triplets));
    }
  }

  /**
   * Compute similarities based on the existing vertex properties,
   * save aggregated similarity as edge property
   * @param graph input graph
   * @param matchCombination relevant: Utils.SIM_GEO_LABEL_STRATEGY or Utils.DEFAULT_VALUE
   * @return graph with edge similarities
   */
  public static Graph<Long, ObjectMap, ObjectMap> computeGraphEdgeSim(
      Graph<Long, ObjectMap, NullValue> graph,
      String matchCombination,
      ExecutionEnvironment env) {
    // sim edge class create TODO
    DataSet<Edge<Long, ObjectMap>> edges = computeSimilarities(graph.getTriplets(), matchCombination)
        .map(new TripletToEdgeMapFunction())
        .map(new AggSimValueEdgeMapFunction(Constants.IGNORE_MISSING_PROPERTIES));

    return Graph.fromDataSet(graph.getVertices(), edges, env);
  }

  /**
   * Join several sets of triplets which are being produced within property similarity computation.
   * Edges where no similarity value is higher than the appropriate threshold are not in the result set.
   * @param tripletDataSet input data sets
   * @return joined dataset with all similarities in an ObjectMap
   */
  @SafeVarargs
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

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicTypeSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets.map(new TypeSimilarityMapper());
  }

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicTrigramSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets.map(new TrigramSimilarityMapper());
  }

  private static DataSet<Triplet<Long, ObjectMap, ObjectMap>> basicGeoSimilarity(
      DataSet<Triplet<Long, ObjectMap, NullValue>> triplets) {
    return triplets.filter(new EmptyGeoCodeFilter())
        .map(new GeoCodeSimMapper(Constants.MAXIMAL_GEO_DISTANCE));
  }

  /**
   * Get a new triplet with an empty ObjectMap as edge value.
   * @param triplet triplet where edge value is NullValue
   * @return result triplet
   */
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
   * @param values property map
   * @return mean similarity value
   */

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

    Preconditions.checkArgument(propCount != 0, "prop count 0 for objectmap: " + values.toString());

    BigDecimal result = new BigDecimal(aggregatedSim / propCount);
    result = result.setScale(10, BigDecimal.ROUND_HALF_UP);

    return result.doubleValue();
  }

  /**
   * Compose similarity values based on weights for each of the properties, missing values are counted as zero.
   * @param values property map
   * @return aggregated similarity value
   */
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

  /**
   * Used for building the similarity computation operator instance.
   * @param <T> data type merge triple (working) or normal triple (to be implemented)
   */
  public static final class SimilarityComputationBuilder<T> {

    private SimilarityFunction<T> function;
    private AggregationMode<T> mode = null;
    private SimilarityStrategy strategy;
    private double threshold;

    public SimilarityComputationBuilder<T> setStrategy(SimilarityStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public SimilarityComputationBuilder<T> setSimilarityFunction(SimilarityFunction<T> function) {
      this.function = function;
      return this;
    }

    public SimilarityComputationBuilder<T> setAggregationMode(AggregationMode<T> mode) {
      this.mode = mode;
      return this;
    }

    public SimilarityComputationBuilder<T> setThreshold(double threshold) {
      this.threshold = threshold;
      return this;
    }

    /**
     * Creates similarity computation operator based on the configured parameters.
     * @return similarity computation operator
     */
    public SimilarityComputation<T> build() {
      // return different implementation for mergetriplet and normal triple
      if (strategy == SimilarityStrategy.MERGE) {
        return new MergeSimilarityComputation<>(function, strategy, threshold);
      } else {
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }
    }

  }
}
