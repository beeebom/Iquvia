
from typing import List, Optional, Dict, Any, Set
from pydantic import BaseModel, Field
from enum import Enum

class DataType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"

class ColumnDefinition(BaseModel):

    name: str
    data_type: DataType = DataType.STRING
    nullable: bool = True
    is_key: bool = False
    source_expression: Optional[str] = None
    description: Optional[str] = None

class Transformation(BaseModel):

    operation: str
    dependencies: List[str] = Field(default_factory=list)  

class SelectTransformation(Transformation):

    operation: str = "select"
    table_name: str
    source_table: Optional[str] = None
    columns: List[ColumnDefinition] = Field(default_factory=list)
    is_distinct: bool = False

class FilterTransformation(Transformation):

    operation: str = "filter"
    table_name: str
    source_table: str
    condition: str

class JoinTransformation(Transformation):

    operation: str = "join"
    table_name: str
    left_table: str
    right_table: str
    join_type: str  
    join_keys: List[str] = Field(default_factory=list)
    join_condition: Optional[str] = None

class AggregationTransformation(Transformation):

    operation: str = "aggregate"
    table_name: str
    source_table: str
    group_by_columns: List[str] = Field(default_factory=list)
    aggregations: Dict[str, str] = Field(default_factory=dict)  

class UnionTransformation(Transformation):

    operation: str = "union"
    table_name: str
    source_tables: List[str] = Field(default_factory=list)

class MappingDefinition(BaseModel):

    mapping_name: str
    source_table: str
    key_column: str
    value_column: str
    filter_condition: Optional[str] = None

class TableDefinition(BaseModel):

    name: str
    columns: List[ColumnDefinition] = Field(default_factory=list)
    primary_keys: List[str] = Field(default_factory=list)
    source_type: str = "external"  
    source_path: Optional[str] = None
    transformations: List[Transformation] = Field(default_factory=list)

class Relationship(BaseModel):

    from_table: str
    to_table: str
    from_columns: List[str]
    to_columns: List[str]
    relationship_type: str = "many_to_one"  

class DataModel(BaseModel):

    tables: Dict[str, TableDefinition] = Field(default_factory=dict)
    mappings: Dict[str, MappingDefinition] = Field(default_factory=dict)
    relationships: List[Relationship] = Field(default_factory=list)
    variables: Dict[str, str] = Field(default_factory=dict)
    execution_order: List[str] = Field(default_factory=list)  
