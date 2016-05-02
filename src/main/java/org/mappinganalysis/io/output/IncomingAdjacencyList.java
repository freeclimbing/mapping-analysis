/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.mappinganalysis.io.output;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * generates a string representation fo an incoming adjacency list
 */
public class IncomingAdjacencyList
  implements GroupReduceFunction<EdgeString, VertexString> {

  @Override
  public void reduce(Iterable<EdgeString> outgoingEdgeLabels,
    Collector<VertexString> collector) throws Exception {

    Boolean first = true;
    Long vertexId = null;

    List<String> adjacencyListEntries = new ArrayList<>();

    for (EdgeString edgeString : outgoingEdgeLabels) {
      if (first) {
        vertexId = edgeString.getTargetId();
        first = false;
      }

      adjacencyListEntries.add("\n  <--" +
        edgeString.getSourceLabel());
    }

    Collections.sort(adjacencyListEntries);

    collector.collect(new VertexString(
      vertexId,
      StringUtils.join(adjacencyListEntries, "")
    ));
  }
}
