
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field

class ConversionOptions(BaseModel):

    fabric_compatible: bool = True
    include_comments: bool = True
    optimize_joins: bool = True

class ConvertRequest(BaseModel):

    script: str = Field(..., description="Qlik script content")
    mode: str = Field(default="transformation", description="Mode: 'extraction' or 'transformation'")
    options: Optional[ConversionOptions] = Field(default_factory=ConversionOptions)

class ExecutionStep(BaseModel):

    step_number: int
    table_name: str
    operation: str
    dependencies: List[str] = Field(default_factory=list)
    description: str

class ConvertResponse(BaseModel):

    success: bool
    pyspark_code: str
    semantic_model: Dict[str, Any]
    execution_plan: List[ExecutionStep]
    warnings: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)

class HealthResponse(BaseModel):

    status: str
    version: str
