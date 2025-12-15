# Examples

This directory contains example Qlik scripts and their corresponding PySpark outputs.

## sample_script.qvs

A comprehensive Qlik Data Load Script demonstrating:

- Variable definitions (LET and SET)
- Mapping tables with ApplyMap
- External file loads
- Calculated fields using Qlik functions (Year, Month, Upper, etc.)
- WHERE clauses for filtering
- LEFT JOIN operations
- GROUP BY aggregations
- INLINE data loads
- DISTINCT selections
- RESIDENT loads

## sample_output.py

The generated PySpark code from `sample_script.qvs`, showing:

- DataFrame creation from various sources
- Column transformations using PySpark functions
- Filtering operations
- Join operations
- Aggregations with groupBy and agg
- Schema definitions for inline data
- Distinct selections

## Usage

### Using the API

```bash
# Start the server
uvicorn app.main:app --reload

# Send request
curl -X POST "http://localhost:8000/api/v1/convert" \
  -H "Content-Type: application/json" \
  -d '{
    "script": "LOAD CustomerID, CustomerName FROM customers.csv;",
    "mode": "transformation",
    "options": {
      "fabric_compatible": true
    }
  }'
```

### Using Python directly

```python
from app.core.parser import QlikParser
from app.core.transformer import ASTTransformer
from app.core.codegen import PySparkCodeGenerator
from app.core.semantic import SemanticModelGenerator

# Read Qlik script
with open('examples/sample_script.qvs', 'r') as f:
    qlik_script = f.read()

# Parse
parser = QlikParser()
ast = parser.parse(qlik_script)

# Transform
transformer = ASTTransformer()
data_model = transformer.transform(ast)

# Generate PySpark code
codegen = PySparkCodeGenerator(fabric_compatible=True)
pyspark_code = codegen.generate(data_model)

# Generate semantic model
semantic_gen = SemanticModelGenerator()
semantic_model = semantic_gen.generate(data_model)

# Output
print(pyspark_code)
print(semantic_model)
```

## Testing

Run the example through the test suite:

```bash
pytest tests/ -v
```

