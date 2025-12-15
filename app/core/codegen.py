
import re
from typing import List, Dict, Optional
from app.models.ir_models import (
    DataModel, TableDefinition, ColumnDefinition,
    SelectTransformation, FilterTransformation, JoinTransformation,
    AggregationTransformation, DataType
)
from app.utils.qlik_functions import QlikFunctionMapper

class PySparkCodeGenerator:

    def __init__(self, fabric_compatible: bool = True):
        self.fabric_compatible = fabric_compatible
        self.function_mapper = QlikFunctionMapper()
        self.indent = "    "

    def generate(self, data_model: DataModel, mode: str = "transformation") -> str:

        code_lines = []

        code_lines.extend(self._generate_header())

        if data_model.variables:
            code_lines.extend(self._generate_variables(data_model.variables))
            code_lines.append("")

        for mapping_name, mapping in data_model.mappings.items():
            code_lines.extend(self._generate_mapping(mapping_name, mapping))
            code_lines.append("")

        for table_name in data_model.execution_order:
            if table_name in data_model.tables:
                table = data_model.tables[table_name]
                code_lines.extend(self._generate_table(table, mode))
                code_lines.append("")

        code_lines.extend(self._generate_footer(data_model))

        return "\n".join(code_lines)

    def _generate_header(self) -> List[str]:

        if self.fabric_compatible:
            return [
                "# Generated PySpark Code - Microsoft Fabric Compatible",
                "from pyspark.sql import SparkSession",
                "from pyspark.sql.functions import *",
                "from pyspark.sql.types import *",
                "from pyspark.sql.window import Window",
                "",
                "# Initialize Spark session (if not already available in Fabric)",
                "# spark = SparkSession.builder.appName('QlikConverter').getOrCreate()",
                ""
            ]
        else:
            return [
                "# Generated PySpark Code",
                "from pyspark.sql import SparkSession",
                "from pyspark.sql.functions import *",
                "from pyspark.sql.types import *",
                "",
                "spark = SparkSession.builder.appName('QlikConverter').getOrCreate()",
                ""
            ]

    def _generate_variables(self, variables: Dict[str, str]) -> List[str]:

        lines = ["# Variables"]
        for var_name, var_value in variables.items():

            python_value = self._convert_expression_to_python(var_value)
            lines.append(f"{var_name} = {python_value}")
        return lines

    def _generate_mapping(self, mapping_name: str, mapping) -> List[str]:

        lines = [f"# Mapping: {mapping_name}"]

        df_name = f"map_{mapping_name}"
        source_code = self._generate_source_load(mapping.source_table, df_name)
        lines.append(source_code)

        lines.append(f"{df_name} = {df_name}.select('{mapping.key_column}', '{mapping.value_column}')")

        if mapping.filter_condition:
            condition = self._convert_expression(mapping.filter_condition)
            lines.append(f"{df_name} = {df_name}.filter({condition})")

        lines.append(f"{df_name}_dict = {df_name}.rdd.collectAsMap()")

        return lines

    def _generate_table(self, table: TableDefinition, mode: str) -> List[str]:

        lines = [f"# Table: {table.name}"]
        df_name = self._to_df_name(table.name)

        if table.source_type == "external":
            lines.append(self._generate_external_load(table, df_name))
        elif table.source_type == "resident":
            lines.append(self._generate_resident_load(table, df_name))
        elif table.source_type == "inline":
            lines.extend(self._generate_inline_load(table, df_name))

        for trans in table.transformations:
            if isinstance(trans, SelectTransformation):
                lines.extend(self._generate_select(trans, df_name))
            elif isinstance(trans, FilterTransformation):
                lines.extend(self._generate_filter(trans, df_name))
            elif isinstance(trans, JoinTransformation):
                lines.extend(self._generate_join(trans, df_name))
            elif isinstance(trans, AggregationTransformation):
                lines.extend(self._generate_aggregation(trans, df_name))

        return lines

    def _generate_external_load(self, table: TableDefinition, df_name: str) -> str:

        source_path = table.source_path or "data.csv"

        if source_path.endswith('.csv') or source_path.endswith('.txt'):
            return f"{df_name} = spark.read.csv('{source_path}', header=True, inferSchema=True)"
        elif source_path.endswith('.parquet'):
            return f"{df_name} = spark.read.parquet('{source_path}')"
        elif source_path.endswith('.json'):
            return f"{df_name} = spark.read.json('{source_path}')"
        elif source_path.endswith('.xlsx') or source_path.endswith('.xls'):
            return f"{df_name} = spark.read.format('excel').load('{source_path}')"
        else:

            return f"{df_name} = spark.read.csv('{source_path}', header=True, inferSchema=True)"

    def _generate_resident_load(self, table: TableDefinition, df_name: str) -> str:

        source_table = table.source_path
        source_df = self._to_df_name(source_table)
        return f"{df_name} = {source_df}"

    def _generate_inline_load(self, table: TableDefinition, df_name: str) -> List[str]:

        lines = []

        schema_fields = []
        for col in table.columns:
            spark_type = self._to_spark_type(col.data_type)
            schema_fields.append(f"StructField('{col.name}', {spark_type}, {col.nullable})")

        schema_str = ", ".join(schema_fields)
        lines.append(f"{df_name}_schema = StructType([{schema_str}])")

        lines.append(f"{df_name}_data = []  # Add inline data here")
        lines.append(f"{df_name} = spark.createDataFrame({df_name}_data, {df_name}_schema)")

        return lines

    def _generate_select(self, trans: SelectTransformation, df_name: str) -> List[str]:

        lines = []

        if trans.is_distinct:
            lines.append(f"{df_name} = {df_name}.distinct()")
            return lines

        select_exprs = []
        for col in trans.columns:
            if col.source_expression:

                expr = self._convert_expression(col.source_expression)
                select_exprs.append(f"{expr}.alias('{col.name}')")
            else:

                select_exprs.append(f"col('{col.name}')")

        if select_exprs:
            select_str = ", ".join(select_exprs)
            lines.append(f"{df_name} = {df_name}.select({select_str})")

        return lines

    def _generate_filter(self, trans: FilterTransformation, df_name: str) -> List[str]:

        condition = self._convert_expression(trans.condition)
        return [f"{df_name} = {df_name}.filter({condition})"]

    def _generate_join(self, trans: JoinTransformation, df_name: str) -> List[str]:

        lines = []

        left_df = self._to_df_name(trans.left_table)
        right_df = self._to_df_name(trans.right_table)

        if trans.join_keys:
            join_cond = " & ".join([f"({left_df}['{key}'] == {right_df}['{key}'])" for key in trans.join_keys])
        elif trans.join_condition:
            join_cond = self._convert_expression(trans.join_condition)
        else:

            join_cond = "True"

        join_type = trans.join_type
        lines.append(f"{df_name} = {left_df}.join({right_df}, {join_cond}, '{join_type}')")

        return lines

    def _generate_aggregation(self, trans: AggregationTransformation, df_name: str) -> List[str]:

        lines = []

        source_df = self._to_df_name(trans.source_table)

        agg_exprs = []
        for col_name, agg_expr in trans.aggregations.items():
            converted_expr = self._convert_expression(agg_expr)
            agg_exprs.append(f"{converted_expr}.alias('{col_name}')")

        group_cols = ", ".join([f"col('{col}')" for col in trans.group_by_columns])
        agg_str = ", ".join(agg_exprs)

        lines.append(f"{df_name} = {source_df}.groupBy({group_cols}).agg({agg_str})")

        return lines

    def _generate_source_load(self, source: str, df_name: str) -> str:

        if '.' in source and any(ext in source for ext in ['.csv', '.txt', '.parquet', '.json']):

            if source.endswith('.csv') or source.endswith('.txt'):
                return f"{df_name} = spark.read.csv('{source}', header=True, inferSchema=True)"
            elif source.endswith('.parquet'):
                return f"{df_name} = spark.read.parquet('{source}')"
            elif source.endswith('.json'):
                return f"{df_name} = spark.read.json('{source}')"
        else:

            source_df = self._to_df_name(source)
            return f"{df_name} = {source_df}"

        return f"{df_name} = spark.read.csv('{source}', header=True, inferSchema=True)"

    def _generate_footer(self, data_model: DataModel) -> List[str]:

        lines = [
            "# Display results (optional)",
            "# Uncomment to view tables:"
        ]

        for table_name in data_model.execution_order:
            df_name = self._to_df_name(table_name)
            lines.append(f"# {df_name}.show()")

        return lines

    def _convert_expression(self, expr: str) -> str:

        expr = self._wrap_column_refs(expr)

        expr = self._convert_functions(expr)

        expr = expr.replace(' AND ', ' & ')
        expr = expr.replace(' and ', ' & ')
        expr = expr.replace(' OR ', ' | ')
        expr = expr.replace(' or ', ' | ')
        expr = expr.replace(' NOT ', ' ~ ')
        expr = expr.replace(' not ', ' ~ ')
        expr = re.sub(r'(?<![=!<>])=(?!=)', '==', expr)
        expr = expr.replace('<>', '!=')

        return expr

    def _convert_functions(self, expr: str) -> str:

        pattern = r'(\w+)\((.*?)\)'

        def replace_func(match):
            func_name = match.group(1)
            args_str = match.group(2)

            args = [arg.strip() for arg in args_str.split(',')]

            pyspark_func = self.function_mapper.map_function(func_name, args)
            return pyspark_func

        expr = re.sub(pattern, replace_func, expr)
        return expr

    def _wrap_column_refs(self, expr: str) -> str:

        if 'col(' in expr or expr.strip().startswith("'") or expr.strip().startswith('"'):
            return expr

        pattern = r'\b([A-Za-z_]\w*)\b(?!\s*\()'

        def wrap_col(match):
            col_name = match.group(1)

            keywords = {'and', 'or', 'not', 'True', 'False', 'None'}
            if col_name.lower() in keywords:
                return col_name
            return f"col('{col_name}')"

        return re.sub(pattern, wrap_col, expr)

    def _convert_expression_to_python(self, expr: str) -> str:

        expr = expr.strip()

        if expr.startswith("'") and expr.endswith("'"):
            return expr

        if expr.isdigit() or re.match(r'^\d+\.\d+$', expr):
            return expr

        if 'Today()' in expr:
            return "datetime.now().date()"
        elif 'Now()' in expr:
            return "datetime.now()"

        return f"'{expr}'"

    def _to_df_name(self, table_name: str) -> str:

        df_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        df_name = re.sub(r'^(\d)', r'_\1', df_name)  
        return f"df_{df_name.lower()}"

    def _to_spark_type(self, data_type: DataType) -> str:

        type_map = {
            DataType.STRING: "StringType()",
            DataType.INTEGER: "IntegerType()",
            DataType.LONG: "LongType()",
            DataType.DOUBLE: "DoubleType()",
            DataType.BOOLEAN: "BooleanType()",
            DataType.DATE: "DateType()",
            DataType.TIMESTAMP: "TimestampType()",
            DataType.DECIMAL: "DecimalType()",
        }
        return type_map.get(data_type, "StringType()")
