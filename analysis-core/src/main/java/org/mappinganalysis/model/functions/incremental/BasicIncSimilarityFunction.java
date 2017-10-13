package org.mappinganalysis.model.functions.incremental;

import org.mappinganalysis.graph.SimilarityFunction;
import org.mappinganalysis.io.impl.DataDomain;
import org.mappinganalysis.model.MergeGeoTriplet;

public class BasicIncSimilarityFunction
    extends SimilarityFunction<MergeGeoTriplet,
    MergeGeoTriplet> {
  private DataDomain domain;

  public BasicIncSimilarityFunction(DataDomain domain) {
    this.domain = domain;
  }

  @Override
  public MergeGeoTriplet map(
      MergeGeoTriplet value) throws Exception {

    return null;
  }
}
