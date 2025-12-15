
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from enum import Enum

class LoadType(str, Enum):
    EXTERNAL = "external"
    RESIDENT = "resident"
    INLINE = "inline"
    MAPPING = "mapping"

class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"

class ASTNode(BaseModel):

    node_type: str

class FieldExpression(ASTNode):

    node_type: str = "field_expression"
    raw_expression: str
    alias: Optional[str] = None
    is_calculated: bool = False

class FunctionCall(ASTNode):

    node_type: str = "function_call"
    function_name: str
    arguments: List[Union[str, 'FunctionCall']]
    alias: Optional[str] = None

class WhereClause(ASTNode):

    node_type: str = "where_clause"
    condition: str

class GroupByClause(ASTNode):

    node_type: str = "group_by"
    fields: List[str]

class JoinClause(ASTNode):

    node_type: str = "join"
    join_type: JoinType
    table_name: str
    on_fields: Optional[List[str]] = None  

class OrderByClause(ASTNode):

    node_type: str = "order_by"
    fields: List[str]
    ascending: bool = True

class LoadStatement(ASTNode):

    node_type: str = "load_statement"
    load_type: LoadType
    table_name: Optional[str] = None
    fields: List[FieldExpression] = Field(default_factory=list)
    source: Optional[str] = None  
    where_clause: Optional[WhereClause] = None
    group_by: Optional[GroupByClause] = None
    order_by: Optional[OrderByClause] = None
    join_clause: Optional[JoinClause] = None
    distinct: bool = False
    preceding_load: Optional['LoadStatement'] = None  
    inline_data: Optional[List[List[str]]] = None
    is_mapping: bool = False

class MappingLoad(ASTNode):

    node_type: str = "mapping_load"
    mapping_name: str
    key_field: str
    value_field: str
    source: str
    where_clause: Optional[WhereClause] = None

class ApplyMapCall(ASTNode):

    node_type: str = "applymap"
    mapping_name: str
    lookup_field: str
    default_value: Optional[str] = None
    alias: Optional[str] = None

class VariableAssignment(ASTNode):

    node_type: str = "variable"
    variable_name: str
    value: str
    is_let: bool = True  

class Script(ASTNode):

    node_type: str = "script"
    statements: List[Union[LoadStatement, MappingLoad, VariableAssignment]] = Field(default_factory=list)

LoadStatement.model_rebuild()
FunctionCall.model_rebuild()
