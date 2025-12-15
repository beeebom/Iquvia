
from fastapi import APIRouter, HTTPException
from app.models.api_models import ConvertRequest, ConvertResponse, HealthResponse, ExecutionStep
from app.core.parser import QlikParser
from app.core.transformer import ASTTransformer
from app.core.codegen import PySparkCodeGenerator
from app.core.semantic import SemanticModelGenerator

router = APIRouter()

@router.get("/health", response_model=HealthResponse)
async def health_check():

    return HealthResponse(status="healthy", version="1.0.0")

@router.post("/convert", response_model=ConvertResponse)
async def convert_script(request: ConvertRequest):

    warnings = []
    errors = []

    try:

        parser = QlikParser()
        ast = parser.parse(request.script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        codegen = PySparkCodeGenerator(
            fabric_compatible=request.options.fabric_compatible
        )
        pyspark_code = codegen.generate(data_model, mode=request.mode)

        semantic_gen = SemanticModelGenerator()
        semantic_model = semantic_gen.generate(data_model)

        execution_plan = _build_execution_plan(data_model)

        return ConvertResponse(
            success=True,
            pyspark_code=pyspark_code,
            semantic_model=semantic_model,
            execution_plan=execution_plan,
            warnings=warnings,
            errors=errors
        )

    except Exception as e:
        errors.append(str(e))
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")

def _build_execution_plan(data_model) -> list:

    plan = []

    for idx, table_name in enumerate(data_model.execution_order, 1):
        if table_name not in data_model.tables:
            continue

        table = data_model.tables[table_name]

        operation = "Load"
        if table.transformations:
            operation = table.transformations[0].operation.capitalize()

        dependencies = []
        for trans in table.transformations:
            dependencies.extend(trans.dependencies)
        dependencies = list(set(dependencies))  

        description = f"Load {table_name}"
        if table.source_type == "external":
            description = f"Load {table_name} from {table.source_path}"
        elif table.source_type == "resident":
            description = f"Transform {table.source_path} into {table_name}"
        elif table.source_type == "inline":
            description = f"Create {table_name} from inline data"

        step = ExecutionStep(
            step_number=idx,
            table_name=table_name,
            operation=operation,
            dependencies=dependencies,
            description=description
        )
        plan.append(step)

    return plan
