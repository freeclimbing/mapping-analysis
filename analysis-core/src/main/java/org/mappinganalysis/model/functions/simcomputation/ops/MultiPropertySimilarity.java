package org.mappinganalysis.model.functions.simcomputation.ops;

import com.google.common.collect.Lists;
import org.mappinganalysis.model.EdgeObjectMapTriplet;
import org.mappinganalysis.model.ObjectMap;
import org.mappinganalysis.model.api.CustomOperation;
import org.mappinganalysis.util.Utils;
import org.simmetrics.StringMetric;

import java.util.ArrayList;

/**
 * Multiple properties are used to compute a single similarity.
 * TODO NOT YET USED
 */
public class MultiPropertySimilarity implements CustomOperation<EdgeObjectMapTriplet> {
  private EdgeObjectMapTriplet triplet;

  @Override
  public void setInput(EdgeObjectMapTriplet inputData) {
    this.triplet = inputData;
  }

  @Override
  public EdgeObjectMapTriplet createResult() {
    StringMetric metric = Utils.getTrigramMetric();
    ArrayList<String> listOne = Lists.newArrayList();
    ArrayList<String> listTwo = Lists.newArrayList();

    ObjectMap source = triplet.getSrcVertex().getValue();
    ObjectMap target = triplet.getTrgVertex().getValue();

    // add title
//    Collections.addAll(listOne, )

    // artist, album

    return null;
  }
}
