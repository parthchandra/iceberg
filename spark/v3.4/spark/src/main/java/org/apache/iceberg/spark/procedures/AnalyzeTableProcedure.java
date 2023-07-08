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
package org.apache.iceberg.spark.procedures;

import java.util.Locale;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.AnalyzeTable;
import org.apache.iceberg.actions.AnalyzeTable.AnalysisMode;
import org.apache.iceberg.spark.actions.AnalyzeTableSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.ProcedureParameter;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.runtime.BoxedUnit;

/**
 * A procedure that analyzes a table.
 *
 * @see SparkActions#analyzeTable(Table)
 */
public class AnalyzeTableProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("mode", DataTypes.StringType),
        ProcedureParameter.optional("output_location", DataTypes.StringType),
        ProcedureParameter.optional("options", STRING_MAP)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("summary", DataTypes.StringType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new Builder<AnalyzeTableProcedure>() {
      @Override
      protected AnalyzeTableProcedure doBuild() {
        return new AnalyzeTableProcedure(tableCatalog());
      }
    };
  }

  private AnalyzeTableProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    String modeAsString = args.isNullAt(1) ? null : args.getString(1);
    String outputLocation = args.isNullAt(2) ? null : args.getString(2);
    MapData options = args.isNullAt(3) ? null : args.getMap(3);

    return withIcebergTable(
        tableIdent,
        table -> {
          AnalyzeTableSparkAction action = actions().analyzeTable(table);

          if (modeAsString != null) {
            action.mode(AnalysisMode.valueOf(modeAsString.toUpperCase(Locale.ROOT)));
          }

          if (outputLocation != null) {
            action.outputLocation(outputLocation);
          }

          if (options != null) {
            options.foreach(
                DataTypes.StringType,
                DataTypes.StringType,
                (key, value) -> {
                  action.option(key.toString(), value.toString());
                  return BoxedUnit.UNIT;
                });
          }

          AnalyzeTable.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private InternalRow[] toOutputRows(AnalyzeTable.Result result) {
    return new InternalRow[] {newInternalRow(UTF8String.fromString(result.toFormattedString()))};
  }

  @Override
  public String description() {
    return "AnalyzeTableProcedure";
  }
}
