package org.mappinganalysis.model.functions.merge;

import org.apache.log4j.Logger;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.functions.simcomputation.MeanAggregationFunction;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;

/**
 * Add similarities to Merge Triplets based on property values.
 */
public class MergeGeoSimilarity
    extends SimilarityFunction<MergeGeoTriplet, MergeGeoTriplet>
    implements Serializable {
  private static final Logger LOG = Logger.getLogger(MergeGeoSimilarity.class);


  private MeanAggregationFunction aggregationFunction;

  /**
   * Default constructor
   */
  public MergeGeoSimilarity() {
    this(new MeanAggregationFunction());
  }

  /**
   * Constructor for custom aggregation function
   * @param aggregationFunction custom
   */
  private MergeGeoSimilarity(MeanAggregationFunction aggregationFunction) {
    this.aggregationFunction = aggregationFunction;
  }

  @Override
  public MergeGeoTriplet map(MergeGeoTriplet triplet) throws Exception {
    MergeGeoTuple src = triplet.getSrcTuple();
    MergeGeoTuple trg = triplet.getTrgTuple();

    Double labelSimilarity = Utils.getSimilarityAndSimplifyForMetric(src.getLabel(),
        trg.getLabel(), Constants.COSINE_TRIGRAM);

    Double geoSimilarity = Utils.getGeoSimilarity(src.getLatitude(),
        src.getLongitude(),
        trg.getLatitude(),
        trg.getLongitude());

    ObjectMap values = new ObjectMap(Constants.GEO);
    if (labelSimilarity != null) {
      values.setLabelSimilarity(labelSimilarity);
    }
    if (geoSimilarity != null) {
      values.setGeoSimilarity(geoSimilarity);
    }

    triplet.setSimilarity(values
        .runOperation(aggregationFunction)
        .getEdgeSimilarity());

    return triplet;
  }
}
