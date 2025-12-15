
import re
from typing import List, Optional, Tuple, Dict
from app.models.ast_models import (
    Script, LoadStatement, MappingLoad, VariableAssignment,
    FieldExpression, WhereClause, GroupByClause, JoinClause,
    OrderByClause, FunctionCall, LoadType, JoinType, ApplyMapCall
)

class QlikParser:

    def __init__(self):
        self.script_lines: List[str] = []
        self.current_line = 0
        self.variables: Dict[str, str] = {}

    def parse(self, script_text: str) -> Script:

        self.script_lines = self._preprocess(script_text)
        self.current_line = 0

        statements = []
        while self.current_line < len(self.script_lines):
            statement = self._parse_statement()
            if statement:
                statements.append(statement)
            else:
                self.current_line += 1

        return Script(statements=statements)

    def _preprocess(self, script_text: str) -> List[str]:

        lines = []
        current_line = ""

        for line in script_text.split('\n'):

            if '//' in line:
                line = line[:line.index('//')]
            if 'REM' in line.upper():
                line = re.sub(r'\bREM\b.*', '', line, flags=re.IGNORECASE)

            line = line.strip()
            if not line:
                continue

            current_line += " " + line

            if line.endswith(';'):
                lines.append(current_line.strip())
                current_line = ""

        if current_line.strip():
            lines.append(current_line.strip())

        return lines

    def _parse_statement(self) -> Optional[object]:

        if self.current_line >= len(self.script_lines):
            return None

        line = self.script_lines[self.current_line].strip()

        if re.match(r'\b(LET|SET)\b', line, re.IGNORECASE):
            return self._parse_variable_assignment()
        elif re.search(r'\bMAPPING\s+LOAD\b', line, re.IGNORECASE):
            return self._parse_mapping_load()
        elif re.search(r'\b(LOAD|LEFT\s+JOIN|INNER\s+JOIN|RIGHT\s+JOIN|OUTER\s+JOIN|JOIN)\b', line, re.IGNORECASE):
            return self._parse_load_statement()

        self.current_line += 1
        return None

    def _parse_variable_assignment(self) -> VariableAssignment:

        line = self.script_lines[self.current_line].strip().rstrip(';')
        self.current_line += 1

        match = re.match(r'\b(LET|SET)\s+(\w+)\s*=\s*(.+)', line, re.IGNORECASE)
        if match:
            is_let = match.group(1).upper() == 'LET'
            var_name = match.group(2)
            value = match.group(3).strip()

            self.variables[var_name] = value

            return VariableAssignment(
                variable_name=var_name,
                value=value,
                is_let=is_let
            )

        return None

    def _parse_mapping_load(self) -> MappingLoad:

        statement_text = self._get_full_statement()

        match = re.search(r'(\w+):\s*MAPPING\s+LOAD', statement_text, re.IGNORECASE)
        mapping_name = match.group(1) if match else "UnnamedMapping"

        fields_match = re.search(r'MAPPING\s+LOAD\s+(.*?)\s+FROM', statement_text, re.IGNORECASE | re.DOTALL)
        fields_text = fields_match.group(1) if fields_match else ""
        fields = [f.strip() for f in fields_text.split(',')]

        key_field = fields[0] if len(fields) > 0 else "key"
        value_field = fields[1] if len(fields) > 1 else "value"

        source_match = re.search(r'FROM\s+(.*?)(?:WHERE|;)', statement_text, re.IGNORECASE)
        source = source_match.group(1).strip() if source_match else ""

        where_clause = self._parse_where_clause(statement_text)

        return MappingLoad(
            mapping_name=mapping_name,
            key_field=key_field,
            value_field=value_field,
            source=source,
            where_clause=where_clause
        )

    def _parse_load_statement(self) -> LoadStatement:

        statement_text = self._get_full_statement()

        load_type = LoadType.EXTERNAL
        join_clause = None

        join_match = re.match(r'(LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|OUTER\s+JOIN|JOIN)\s*(\((\w+)\))?\s+LOAD', 
                             statement_text, re.IGNORECASE)
        if join_match:
            join_type_str = join_match.group(1).strip().upper()
            if 'LEFT' in join_type_str:
                join_type_str = 'left'
            elif 'RIGHT' in join_type_str:
                join_type_str = 'right'
            elif 'OUTER' in join_type_str:
                join_type_str = 'outer'
            elif 'INNER' in join_type_str or join_type_str == 'JOIN':
                join_type_str = 'inner'
            join_table = join_match.group(3) if join_match.group(3) else None

            join_clause = JoinClause(
                join_type=JoinType(join_type_str),
                table_name=join_table,
                on_fields=None  
            )

        distinct = bool(re.search(r'\bDISTINCT\b', statement_text, re.IGNORECASE))

        table_match = re.match(r'(\w+):\s*(?:(?:LEFT|RIGHT|INNER|OUTER)?\s*JOIN\s*)?LOAD', statement_text, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else None

        fields = self._parse_fields(statement_text)

        source = None
        inline_data = None

        if re.search(r'\bFROM\s+\[', statement_text, re.IGNORECASE):

            load_type = LoadType.EXTERNAL
            source_match = re.search(r'FROM\s+\[([^\]]+)\]', statement_text, re.IGNORECASE)
            if source_match:
                source = source_match.group(1)

        elif re.search(r'\bFROM\b', statement_text, re.IGNORECASE):

            load_type = LoadType.EXTERNAL
            source_match = re.search(r'FROM\s+([\w\./\\:]+)', statement_text, re.IGNORECASE)
            if source_match:
                source = source_match.group(1)

        elif re.search(r'\bRESIDENT\b', statement_text, re.IGNORECASE):

            load_type = LoadType.RESIDENT
            resident_match = re.search(r'RESIDENT\s+(\w+)', statement_text, re.IGNORECASE)
            if resident_match:
                source = resident_match.group(1)

        elif re.search(r'\bINLINE\b', statement_text, re.IGNORECASE):

            load_type = LoadType.INLINE
            inline_data = self._parse_inline_data(statement_text)

        where_clause = self._parse_where_clause(statement_text)
        group_by = self._parse_group_by(statement_text)
        order_by = self._parse_order_by(statement_text)

        return LoadStatement(
            load_type=load_type,
            table_name=table_name,
            fields=fields,
            source=source,
            inline_data=inline_data,
            where_clause=where_clause,
            group_by=group_by,
            order_by=order_by,
            join_clause=join_clause,
            distinct=distinct
        )

    def _get_full_statement(self) -> str:

        statement = self.script_lines[self.current_line]
        self.current_line += 1
        return statement

    def _parse_fields(self, statement_text: str) -> List[FieldExpression]:

        fields_match = re.search(
            r'LOAD(?:\s+DISTINCT)?\s+(.*?)\s+(?:FROM|RESIDENT|INLINE|WHERE|GROUP\s+BY|ORDER\s+BY|;)',
            statement_text,
            re.IGNORECASE | re.DOTALL
        )

        if not fields_match:
            return []

        fields_text = fields_match.group(1).strip()

        if fields_text.strip() == '*':
            return [FieldExpression(raw_expression="*", alias=None)]

        fields = self._smart_split(fields_text, ',')

        field_expressions = []
        for field in fields:
            field = field.strip()
            if not field:
                continue

            alias_match = re.search(r'\s+(?:AS|as)\s+(\w+)$', field, re.IGNORECASE)
            if alias_match:
                alias = alias_match.group(1)
                expression = field[:alias_match.start()].strip()
            else:
                alias = None
                expression = field

            is_calculated = bool(re.search(r'[+\-*/()]|\b(?:If|Year|Month|Upper|Lower|Sum|Count|Avg)\b', 
                                          expression, re.IGNORECASE))

            field_expressions.append(FieldExpression(
                raw_expression=expression,
                alias=alias,
                is_calculated=is_calculated
            ))

        return field_expressions

    def _parse_where_clause(self, statement_text: str) -> Optional[WhereClause]:

        where_match = re.search(
            r'\bWHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|;|$)',
            statement_text,
            re.IGNORECASE | re.DOTALL
        )
        if where_match:
            condition = where_match.group(1).strip()
            return WhereClause(condition=condition)
        return None

    def _parse_group_by(self, statement_text: str) -> Optional[GroupByClause]:

        group_match = re.search(
            r'\bGROUP\s+BY\s+(.*?)(?:\s+ORDER\s+BY|;|$)',
            statement_text,
            re.IGNORECASE
        )
        if group_match:
            fields_text = group_match.group(1).strip()
            fields = [f.strip() for f in fields_text.split(',')]
            return GroupByClause(fields=fields)
        return None

    def _parse_order_by(self, statement_text: str) -> Optional[OrderByClause]:

        order_match = re.search(
            r'\bORDER\s+BY\s+(.*?)(?:;|$)',
            statement_text,
            re.IGNORECASE
        )
        if order_match:
            fields_text = order_match.group(1).strip()

            ascending = 'DESC' not in fields_text.upper()
            fields_text = fields_text.replace('DESC', '').replace('ASC', '')
            fields = [f.strip() for f in fields_text.split(',')]
            return OrderByClause(fields=fields, ascending=ascending)
        return None

    def _parse_inline_data(self, statement_text: str) -> List[List[str]]:

        inline_match = re.search(
            r'INLINE\s*\[(.*?)\]',
            statement_text,
            re.IGNORECASE | re.DOTALL
        )
        if not inline_match:
            return []

        data_text = inline_match.group(1).strip()
        rows = []

        for line in data_text.split('\n'):
            line = line.strip()
            if not line:
                continue

            if ',' in line:
                values = [v.strip() for v in line.split(',')]
            else:
                values = [v.strip() for v in re.split(r'\s+', line)]
            rows.append(values)

        return rows

    def _smart_split(self, text: str, delimiter: str) -> List[str]:

        parts = []
        current = ""
        paren_depth = 0
        in_quotes = False
        quote_char = None

        for char in text:
            if char in ('"', "'") and (not in_quotes or char == quote_char):
                in_quotes = not in_quotes
                quote_char = char if in_quotes else None
                current += char
            elif char == '(' and not in_quotes:
                paren_depth += 1
                current += char
            elif char == ')' and not in_quotes:
                paren_depth -= 1
                current += char
            elif char == delimiter and paren_depth == 0 and not in_quotes:
                parts.append(current)
                current = ""
            else:
                current += char

        if current:
            parts.append(current)

        return parts

    def _substitute_variables(self, text: str) -> str:

        for var_name, var_value in self.variables.items():

            text = text.replace(f"$({var_name})", var_value)

            text = re.sub(rf'\$\({var_name}\)', var_value, text)
        return text
