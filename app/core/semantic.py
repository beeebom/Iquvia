
from typing import Dict, List, Any, Optional
from app.models.ir_models import DataModel, TableDefinition, ColumnDefinition, Relationship

class SemanticModelGenerator:

    def generate(self, data_model: DataModel) -> Dict[str, Any]:

        semantic_model = {
            "name": "QlikConvertedModel",
            "description": "Semantic model generated from Qlik script",
            "version": "1.0",
            "tables": self._generate_tables(data_model),
            "relationships": self._generate_relationships(data_model),
            "measures": self._generate_measures(data_model),
            "metadata": {
                "source": "Qlik Data Load Script",
                "execution_order": data_model.execution_order
            }
        }

        return semantic_model

    def _generate_tables(self, data_model: DataModel) -> List[Dict[str, Any]]:

        tables = []

        for table_name, table_def in data_model.tables.items():
            table_json = {
                "name": table_name,
                "source": {
                    "type": table_def.source_type,
                    "path": table_def.source_path
                },
                "columns": self._generate_columns(table_def.columns),
                "primaryKey": table_def.primary_keys
            }

            if table_def.source_type == "external":
                table_json["source"]["format"] = self._detect_format(table_def.source_path)

            tables.append(table_json)

        return tables

    def _generate_columns(self, columns: List[ColumnDefinition]) -> List[Dict[str, Any]]:

        column_list = []

        for col in columns:
            col_json = {
                "name": col.name,
                "dataType": col.data_type.value,
                "nullable": col.nullable,
                "isKey": col.is_key
            }

            if col.source_expression:
                col_json["expression"] = col.source_expression
                col_json["isCalculated"] = True
            else:
                col_json["isCalculated"] = False

            if col.description:
                col_json["description"] = col.description

            column_list.append(col_json)

        return column_list

    def _generate_relationships(self, data_model: DataModel) -> List[Dict[str, Any]]:

        relationships = []

        for rel in data_model.relationships:
            rel_json = {
                "name": f"{rel.from_table}_to_{rel.to_table}",
                "fromTable": rel.from_table,
                "fromColumn": rel.from_columns[0] if rel.from_columns else None,
                "toTable": rel.to_table,
                "toColumn": rel.to_columns[0] if rel.to_columns else None,
                "cardinality": self._map_cardinality(rel.relationship_type),
                "crossFilterDirection": "single",
                "isActive": True
            }

            if len(rel.from_columns) > 1:
                rel_json["fromColumns"] = rel.from_columns
                rel_json["toColumns"] = rel.to_columns

            relationships.append(rel_json)

        return relationships

    def _generate_measures(self, data_model: DataModel) -> List[Dict[str, Any]]:

        measures = []

        for table_name, table_def in data_model.tables.items():
            for trans in table_def.transformations:
                if hasattr(trans, 'aggregations'):
                    for col_name, agg_expr in trans.aggregations.items():
                        measure = {
                            "name": f"{table_name}_{col_name}",
                            "expression": agg_expr,
                            "table": table_name,
                            "dataType": "double",
                            "formatString": "#,##0.00"
                        }
                        measures.append(measure)

        return measures

    def _detect_format(self, path: Optional[str]) -> str:

        if not path:
            return "unknown"

        path_lower = path.lower()
        if path_lower.endswith('.csv') or path_lower.endswith('.txt'):
            return "csv"
        elif path_lower.endswith('.parquet'):
            return "parquet"
        elif path_lower.endswith('.json'):
            return "json"
        elif path_lower.endswith('.xlsx') or path_lower.endswith('.xls'):
            return "excel"
        else:
            return "unknown"

    def _map_cardinality(self, rel_type: str) -> str:

        mapping = {
            "one_to_one": "1:1",
            "one_to_many": "1:*",
            "many_to_one": "*:1",
            "many_to_many": "*:*"
        }
        return mapping.get(rel_type, "1:*")
