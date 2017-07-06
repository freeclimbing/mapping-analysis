package org.mappinganalysis.model.functions.merge;

import com.google.common.collect.Maps;
import org.mappinganalysis.graph.AggregationMode;
import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.model.MergeGeoTriplet;
import org.mappinganalysis.model.MergeGeoTuple;
import org.mappinganalysis.util.Constants;
import org.mappinganalysis.util.Utils;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Add similarities to Merge Triplets based on property values.
 */
public class MergeGeoSimilarity
    extends SimilarityFunction<MergeGeoTriplet, MergeGeoTriplet>
    implements Serializable {
  private AggregationMode<MergeGeoTriplet> mode;

  public MergeGeoSimilarity(AggregationMode<MergeGeoTriplet> mode) {
    this.mode = mode;
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

    HashMap<String, Double> values = Maps.newHashMap();
    values.put(Constants.LABEL, labelSimilarity);
    values.put(Constants.GEO, geoSimilarity);

    triplet.setSimilarity(mode.compute(values));

    return triplet;
  }
}
