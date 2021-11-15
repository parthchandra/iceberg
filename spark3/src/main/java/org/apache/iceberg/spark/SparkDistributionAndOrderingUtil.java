/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark;

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.transforms.SortOrderVisitor;
import org.apache.iceberg.util.SortOrderUtil;
import org.apache.spark.sql.connector.distributions.ClusteredDistribution;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.distributions.OrderedDistribution;
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.NullOrdering;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;

import static org.apache.iceberg.MetadataColumns.FILE_PATH;
import static org.apache.iceberg.MetadataColumns.PARTITION_COLUMN_NAME;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.MetadataColumns.SPEC_ID;
import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;

public class SparkDistributionAndOrderingUtil {

  private SparkDistributionAndOrderingUtil() {
  }

  public static Distribution buildRequiredDistribution(Table table, DistributionMode distributionMode) {
    return buildRequiredDistribution(table.schema(), table.spec(), distributionMode, table.sortOrder());
  }

  public static Distribution buildRequiredDistribution(Schema schema, PartitionSpec spec,
                                                       DistributionMode distributionMode,
                                                       org.apache.iceberg.SortOrder sortOrder) {
    switch (distributionMode) {
      case NONE:
        return Distributions.unspecified();

      case HASH:
        if (spec.isUnpartitioned()) {
          return Distributions.unspecified();
        } else {
          return Distributions.clustered(Spark3Util.toTransforms(spec));
        }

      case RANGE:
        if (spec.isUnpartitioned() && sortOrder.isUnsorted()) {
          return Distributions.unspecified();
        } else {
          org.apache.iceberg.SortOrder requiredSortOrder = SortOrderUtil.buildSortOrder(schema, spec, sortOrder);
          return Distributions.ordered(convert(requiredSortOrder));
        }

      default:
        throw new IllegalArgumentException("Unsupported distribution mode: " + distributionMode);
    }
  }

  public static SortOrder[] buildRequiredOrdering(Table table, Distribution distribution) {
    return buildRequiredOrdering(table.schema(), table.spec(), distribution, table.sortOrder());
  }

  public static SortOrder[] buildRequiredOrdering(Schema schema, PartitionSpec spec,
                                                  Distribution distribution,
                                                  org.apache.iceberg.SortOrder sortOrder) {
    if (distribution instanceof OrderedDistribution) {
      OrderedDistribution orderedDistribution = (OrderedDistribution) distribution;
      return orderedDistribution.ordering();

    } else {
      org.apache.iceberg.SortOrder requiredSortOrder = SortOrderUtil.buildSortOrder(schema, spec, sortOrder);
      return convert(requiredSortOrder);
    }
  }

  // TODO: support command specific distributions
  public static Distribution buildPositionDeltaDistribution(Table table, Command command,
                                                            DistributionMode distributionMode) {
    if (distributionMode == DistributionMode.NONE) {
      return Distributions.unspecified();
    }

    if (command == Command.DELETE && !table.spec().isUnpartitioned()) {
      // cluster deletes by spec and partition,
      // assuming all deletes for a single partition will fit into one Spark write task
      NamedReference specId = Expressions.column(SPEC_ID.name());
      NamedReference partition = Expressions.column(PARTITION_COLUMN_NAME);
      Expression[] clustering = new Expression[]{specId, partition};
      return Distributions.clustered(clustering);
    }

    // append spec ID and partition metadata columns to data distribution for UPDATE and MERGE commands
    // these metadata columns will be null for new records that have to be inserted
    Distribution dataDistribution = buildRequiredDistribution(table, distributionMode);

    if (dataDistribution instanceof ClusteredDistribution) {
      NamedReference specId = Expressions.column(SPEC_ID.name());
      NamedReference partition = Expressions.column(PARTITION_COLUMN_NAME);
      Expression[] deleteClustering = new Expression[]{specId, partition};
      Expression[] dataClustering = ((ClusteredDistribution) dataDistribution).clustering();
      Expression[] clustering = ObjectArrays.concat(deleteClustering, dataClustering, Expression.class);
      return Distributions.clustered(clustering);

    } else if (dataDistribution instanceof OrderedDistribution) {
      SortOrder specId = Expressions.sort(Expressions.column(SPEC_ID.name()), SortDirection.ASCENDING);
      SortOrder partition = Expressions.sort(Expressions.column(PARTITION_COLUMN_NAME), SortDirection.ASCENDING);
      SortOrder file = Expressions.sort(Expressions.column(FILE_PATH.name()), SortDirection.ASCENDING);
      SortOrder[] deleteOrdering = new SortOrder[]{specId, partition, file};
      SortOrder[] dataOrdering = ((OrderedDistribution) dataDistribution).ordering();
      SortOrder[] ordering = ObjectArrays.concat(deleteOrdering, dataOrdering, SortOrder.class);
      return Distributions.ordered(ordering);

    } else if (dataDistribution instanceof UnspecifiedDistribution) {
      return Distributions.unspecified();

    } else {
      throw new IllegalArgumentException("Unexpected data distribution type: " + dataDistribution);
    }
  }

  public static SortOrder[] buildPositionDeltaRequiredOrdering(Table table, Command command,
                                                               Distribution distribution) {
    // the spec requires position delete files to be sorted by file and pos
    SortOrder specId = Expressions.sort(Expressions.column(SPEC_ID.name()), SortDirection.ASCENDING);
    SortOrder file = Expressions.sort(Expressions.column(FILE_PATH.name()), SortDirection.ASCENDING);
    SortOrder pos = Expressions.sort(Expressions.column(ROW_POSITION.name()), SortDirection.ASCENDING);
    SortOrder[] deleteOrdering = new SortOrder[]{specId, file, pos};

    if (command == Command.DELETE) {
      return deleteOrdering;
    } else {
      // all metadata columns like spec, file, pos will be null for new data records
      SortOrder[] dataOrdering = buildRequiredOrdering(table, distribution);
      return ObjectArrays.concat(deleteOrdering, dataOrdering, SortOrder.class);
    }
  }

  public static Distribution buildCopyOnWriteRequiredDistribution(Table table, Command command,
                                                                  DistributionMode distributionMode) {
    if (distributionMode == DistributionMode.NONE) {
      return Distributions.unspecified();
    } else if (command == Command.DELETE) {
      NamedReference file = Expressions.column(FILE_PATH.name());
      Expression[] clustering = new Expression[]{file};
      return Distributions.clustered(clustering);
    } else {
      return buildRequiredDistribution(table, distributionMode);
    }
  }

  public static SortOrder[] buildCopyOnWriteRequiredOrdering(Table table, Command command,
                                                             Distribution distribution) {
    if (command == Command.DELETE) {
      SortOrder file = Expressions.sort(Expressions.column(FILE_PATH.name()), SortDirection.ASCENDING);
      SortOrder pos = Expressions.sort(Expressions.column(ROW_POSITION.name()), SortDirection.ASCENDING);
      return new SortOrder[]{file, pos};
    } else {
      return buildRequiredOrdering(table, distribution);
    }
  }

  public static org.apache.iceberg.SortOrder toSortOrder(Schema schema, SortOrder[] ordering) {
    org.apache.iceberg.SortOrder.Builder builder = org.apache.iceberg.SortOrder.builderFor(schema);
    rebuildSortOrder(builder, ordering);
    return builder.build();
  }

  public static void rebuildSortOrder(org.apache.iceberg.SortOrderBuilder<?> builder,
                                      SortOrder[] orderFields) {
    Stream.of(orderFields).forEach(field -> {
      Term term = Spark3Util.toIcebergTerm(field.expression());
      NullOrder nullOrder = field.nullOrdering() == NullOrdering.NULLS_FIRST ? NULLS_FIRST : NULLS_LAST;
      if (field.direction() == SortDirection.ASCENDING) {
        builder.asc(term, nullOrder);
      } else {
        builder.desc(term, nullOrder);
      }
    });
  }

  public static SortOrder[] convert(org.apache.iceberg.SortOrder sortOrder) {
    List<OrderField> converted = SortOrderVisitor.visit(sortOrder, new SortOrderToSpark(sortOrder.schema()));
    return converted.toArray(new OrderField[0]);
  }
}
