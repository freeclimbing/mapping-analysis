package org.mappinganalysis.model.functions.blocking.tfidf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Edge;

/**
 * Created by markus on 6/15/17.
 */
public class SupportFilterFunction implements FilterFunction<Edge<Long, Integer>> {
  private Integer support;

  public SupportFilterFunction(Integer support) {
    this.support = support;
  }

  @Override
  public boolean filter(Edge<Long, Integer> value) throws Exception {
//    if (value.getSource() == 12960L || value.getTarget() == 12960L) {
//      System.out.println("supp check: " + value.toString());
//    }
    return value.f2 > support - 1;
  }
}
