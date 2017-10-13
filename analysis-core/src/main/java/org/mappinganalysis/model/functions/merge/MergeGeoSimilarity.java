package org.mappinganalysis.model.functions.merge;

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
  public MergeGeoSimilarity(MeanAggregationFunction aggregationFunction) {
    this.aggregationFunction = aggregationFunction;
  }

  @Override
  public MergeGeoTriplet map(MergeGeoTriplet triplet) throws Exception {
    MergeGeoTuple src = triplet.getSrcTuple();
    MergeGeoTuple trg = triplet.getTrgTuple();

    Double labelSimilarity = Utils.getLabelSimilarity(src.getLabel(),
        trg.getLabel());

    Double geoSimilarity = Utils.getGeoSimilarity(src.getLatitude(),
        src.getLongitude(),
        trg.getLatitude(),
        trg.getLongitude());

    ObjectMap values = new ObjectMap(Constants.GEO);
    values.setLabelSimilarity(labelSimilarity);
    if (geoSimilarity != null) {
      values.setGeoSimilarity(geoSimilarity);
    }

    triplet.setSimilarity(values
        .runOperation(aggregationFunction)
        .getEdgeSimilarity());

    return triplet;
  }
}
