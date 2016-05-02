package org.mappinganalysis.io.output;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConcatStrings implements GroupReduceFunction<String, String> {
  @Override
  public void reduce(Iterable<String> strings, Collector<String> collector) throws Exception {
    List<String> selectedStrings = new ArrayList<>();

    for (String string : strings) {
      selectedStrings.add(string);
    }

    Collections.sort(selectedStrings);
    collector.collect(StringUtils.join(selectedStrings, "\n"));
  }
}