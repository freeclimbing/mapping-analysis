package org.mappinganalysis.model.functions.simcomputation.ops;

import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.api.SimilarityOperation;

/**
 * Compute similarity for a single property.
 */
public class SinglePropertySimilarity implements SimilarityOperation<EdgeObjectMapTriplet> {
  private EdgeObjectMapTriplet triplet;

  @Override
  public void setInput(EdgeObjectMapTriplet inputData) {
    this.triplet = inputData;
  }

  @Override
  public EdgeObjectMapTriplet createResult() {
    if (triplet.getSrcVertex().getValue().getLanguage()
      .equals(triplet.getTrgVertex().getValue().getLanguage())) {
      triplet.getEdge().getValue().setLanguageSimilarity(1D);

      return triplet;
    } else {
      return triplet;
    }
  }
}
