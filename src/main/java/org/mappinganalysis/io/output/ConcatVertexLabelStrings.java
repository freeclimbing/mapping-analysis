package org.mappinganalysis.io.output;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.util.Collector;

import java.util.*;

public class ConcatVertexLabelStrings implements GroupReduceFunction<VertexLabelString, String> {

  @Override
  public void reduce(Iterable<VertexLabelString> clusterVertices,
                     Collector<String> collector) throws Exception {
    List<String> vertexStringList = new ArrayList<>();
    Long id = null;

    for (VertexLabelString clusterVertex : clusterVertices) {
      if (id == null || (long) id != clusterVertex.getId()) {
        id = clusterVertex.getId();
        vertexStringList.add("(" + clusterVertex.getLabel() + ": ");
      }
      vertexStringList.add(clusterVertex.getValue());
    }

    vertexStringList = joinClusterLines(vertexStringList);
    Collections.sort(vertexStringList);
    vertexStringList = splitClusterVertices(vertexStringList);

    collector.collect(StringUtils.join(vertexStringList, "\n"));
  }

  private List<String> splitClusterVertices(List<String> vertexStringList) {
    List<String> result = Lists.newArrayList();

    for (String s : vertexStringList) {
      s = s.replaceAll("\\#\\#", "\n  ");
      result.add(s);
    }

    return result;
  }

  private List<String> joinClusterLines(List<String> vertexStringList) {

    String tmpKey = "";
    HashMap<String, String> resultMap = Maps.newHashMap();


    for (String s : vertexStringList) {
      if (s.startsWith("(")) {
        if (!resultMap.containsKey(s)) {
          resultMap.put(s, "");
        }
        tmpKey = s;
      }
      if (s.startsWith("##")) {
        resultMap.put(tmpKey, resultMap.get(tmpKey) + s);
      }
    }

    ArrayList<String> resultList = Lists.newArrayList();
    for (Map.Entry<String, String> entry : resultMap.entrySet()) {
      resultList.add(entry.getKey() + entry.getValue());
    }
    return resultList;
  }
}
