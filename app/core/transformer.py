
import re
from typing import List, Dict, Set, Optional, Tuple
from app.models.ast_models import (
    Script, LoadStatement, MappingLoad, VariableAssignment,
    FieldExpression, LoadType, JoinType
)
from app.models.ir_models import (
    DataModel, TableDefinition, ColumnDefinition, Relationship,
    SelectTransformation, FilterTransformation, JoinTransformation,
    AggregationTransformation, MappingDefinition, DataType
)

class ASTTransformer:

    def __init__(self):
        self.data_model = DataModel()
        self.table_counter = 0
        self.column_types: Dict[str, DataType] = {}

    def transform(self, ast: Script) -> DataModel:

        for statement in ast.statements:
            if isinstance(statement, VariableAssignment):
                self._process_variable(statement)
            elif isinstance(statement, MappingLoad):
                self._process_mapping(statement)
            elif isinstance(statement, LoadStatement):
                self._process_load_statement(statement)

        self._build_execution_order()

        self._detect_relationships()

        return self.data_model

    def _process_variable(self, var_stmt: VariableAssignment):

        self.data_model.variables[var_stmt.variable_name] = var_stmt.value

    def _process_mapping(self, mapping_stmt: MappingLoad):

        mapping_def = MappingDefinition(
            mapping_name=mapping_stmt.mapping_name,
            source_table=mapping_stmt.source,
            key_column=mapping_stmt.key_field,
            value_column=mapping_stmt.value_field,
            filter_condition=mapping_stmt.where_clause.condition if mapping_stmt.where_clause else None
        )
        self.data_model.mappings[mapping_stmt.mapping_name] = mapping_def

    def _process_load_statement(self, load_stmt: LoadStatement):

        table_name = load_stmt.table_name or self._generate_table_name()

        table = TableDefinition(
            name=table_name,
            source_type=load_stmt.load_type.value
        )

        if load_stmt.load_type == LoadType.EXTERNAL:
            self._process_external_load(load_stmt, table)
        elif load_stmt.load_type == LoadType.RESIDENT:
            self._process_resident_load(load_stmt, table)
        elif load_stmt.load_type == LoadType.INLINE:
            self._process_inline_load(load_stmt, table)

        if load_stmt.join_clause:
            self._process_join(load_stmt, table)

        if load_stmt.where_clause:
            filter_trans = FilterTransformation(
                table_name=table_name,
                source_table=load_stmt.source or table_name,
                condition=self._convert_condition(load_stmt.where_clause.condition),
                dependencies=[load_stmt.source] if load_stmt.source else []
            )
            table.transformations.append(filter_trans)

        if load_stmt.group_by:
            self._process_group_by(load_stmt, table)

        if load_stmt.distinct:
            table.transformations.append(SelectTransformation(
                table_name=table_name,
                source_table=table_name,
                columns=table.columns,
                is_distinct=True,
                dependencies=[table_name]
            ))

        self.data_model.tables[table_name] = table

    def _process_external_load(self, load_stmt: LoadStatement, table: TableDefinition):

        table.source_path = load_stmt.source

        for field_expr in load_stmt.fields:
            if field_expr.raw_expression == '*':

                continue

            col_name = field_expr.alias or self._extract_field_name(field_expr.raw_expression)
            data_type = self._infer_data_type(field_expr.raw_expression)

            column = ColumnDefinition(
                name=col_name,
                data_type=data_type,
                source_expression=field_expr.raw_expression if field_expr.is_calculated else None
            )
            table.columns.append(column)
            self.column_types[col_name] = data_type

    def _process_resident_load(self, load_stmt: LoadStatement, table: TableDefinition):

        source_table = load_stmt.source
        table.source_path = source_table

        select_trans = SelectTransformation(
            table_name=table.name,
            source_table=source_table,
            dependencies=[source_table]
        )

        for field_expr in load_stmt.fields:
            if field_expr.raw_expression == '*':

                if source_table in self.data_model.tables:
                    select_trans.columns = self.data_model.tables[source_table].columns.copy()
                continue

            col_name = field_expr.alias or self._extract_field_name(field_expr.raw_expression)
            data_type = self._infer_data_type(field_expr.raw_expression)

            column = ColumnDefinition(
                name=col_name,
                data_type=data_type,
                source_expression=field_expr.raw_expression if field_expr.is_calculated else None
            )
            select_trans.columns.append(column)
            table.columns.append(column)

        table.transformations.append(select_trans)

    def _process_inline_load(self, load_stmt: LoadStatement, table: TableDefinition):

        table.source_type = "inline"

        if load_stmt.inline_data and len(load_stmt.inline_data) > 0:
            headers = load_stmt.inline_data[0]

            for header in headers:
                column = ColumnDefinition(
                    name=header,
                    data_type=DataType.STRING,  
                    nullable=True
                )
                table.columns.append(column)

    def _process_join(self, load_stmt: LoadStatement, table: TableDefinition):

        join_clause = load_stmt.join_clause

        if join_clause.table_name:

            left_table = join_clause.table_name
        else:

            left_table = self._get_previous_table()

        right_table = table.name

        join_keys = join_clause.on_fields or self._detect_join_keys(left_table, right_table)

        join_trans = JoinTransformation(
            table_name=table.name,
            left_table=left_table,
            right_table=right_table,
            join_type=join_clause.join_type.value,
            join_keys=join_keys,
            dependencies=[left_table, right_table]
        )

        table.transformations.append(join_trans)

    def _process_group_by(self, load_stmt: LoadStatement, table: TableDefinition):

        group_by = load_stmt.group_by

        agg_map = {}
        group_cols = group_by.fields

        for field_expr in load_stmt.fields:
            if self._is_aggregate_expression(field_expr.raw_expression):
                col_name = field_expr.alias or self._extract_field_name(field_expr.raw_expression)
                agg_map[col_name] = field_expr.raw_expression

        agg_trans = AggregationTransformation(
            table_name=table.name,
            source_table=load_stmt.source or table.name,
            group_by_columns=group_cols,
            aggregations=agg_map,
            dependencies=[load_stmt.source] if load_stmt.source else []
        )

        table.transformations.append(agg_trans)

    def _detect_join_keys(self, left_table: str, right_table: str) -> List[str]:

        if left_table not in self.data_model.tables or right_table not in self.data_model.tables:
            return []

        left_cols = {col.name for col in self.data_model.tables[left_table].columns}
        right_cols = {col.name for col in self.data_model.tables[right_table].columns}

        common_cols = left_cols & right_cols

        return list(common_cols)

    def _detect_relationships(self):

        table_names = list(self.data_model.tables.keys())

        for i, table1_name in enumerate(table_names):
            for table2_name in table_names[i+1:]:
                table1 = self.data_model.tables[table1_name]
                table2 = self.data_model.tables[table2_name]

                cols1 = {col.name for col in table1.columns}
                cols2 = {col.name for col in table2.columns}
                common = cols1 & cols2

                if common:

                    rel = Relationship(
                        from_table=table1_name,
                        to_table=table2_name,
                        from_columns=list(common),
                        to_columns=list(common),
                        relationship_type="many_to_one"
                    )
                    self.data_model.relationships.append(rel)

    def _build_execution_order(self):

        visited = set()
        order = []

        def visit(table_name: str):
            if table_name in visited:
                return
            visited.add(table_name)

            if table_name not in self.data_model.tables:
                return

            table = self.data_model.tables[table_name]

            for trans in table.transformations:
                for dep in trans.dependencies:
                    if dep != table_name:  
                        visit(dep)

            if table.source_type == "resident" and table.source_path:
                visit(table.source_path)

            order.append(table_name)

        for table_name in self.data_model.tables.keys():
            visit(table_name)

        self.data_model.execution_order = order

    def _generate_table_name(self) -> str:

        self.table_counter += 1
        return f"Table{self.table_counter}"

    def _get_previous_table(self) -> str:

        if self.data_model.execution_order:
            return self.data_model.execution_order[-1]
        return "UnknownTable"

    def _extract_field_name(self, expression: str) -> str:

        expression = expression.strip().strip('"').strip("'")

        if re.match(r'^\w+$', expression):
            return expression

        func_match = re.search(r'\w+\((.+?)\)', expression)
        if func_match:
            inner = func_match.group(1)

            return self._extract_field_name(inner)

        return expression.replace(' ', '_').replace('(', '').replace(')', '')

    def _infer_data_type(self, expression: str) -> DataType:

        expression_lower = expression.lower()

        if any(func in expression_lower for func in ['date', 'today', 'now', 'timestamp']):
            return DataType.TIMESTAMP if 'timestamp' in expression_lower or 'now' in expression_lower else DataType.DATE

        if any(func in expression_lower for func in ['year', 'month', 'day', 'count', 'sum', 'round', 'floor', 'ceil']):
            return DataType.LONG if any(f in expression_lower for f in ['year', 'month', 'day', 'count']) else DataType.DOUBLE

        if any(func in expression_lower for func in ['upper', 'lower', 'trim', 'concat', 'substr']):
            return DataType.STRING

        return DataType.STRING

    def _is_aggregate_expression(self, expression: str) -> bool:

        agg_functions = ['sum', 'count', 'avg', 'min', 'max', 'first', 'last']
        expression_lower = expression.lower()
        return any(func in expression_lower for func in agg_functions)

    def _convert_condition(self, condition: str) -> str:

        condition = condition.replace(' AND ', ' & ')
        condition = condition.replace(' OR ', ' | ')
        condition = condition.replace(' NOT ', ' ~ ')
        condition = condition.replace('=', '==')
        condition = condition.replace('<>', '!=')

        return condition
