# Qlik-to-PySpark Converter - Project Structure

## Clean Production-Ready Structure

```
iquvia/
├── app/                        # Core application
│   ├── __init__.py
│   ├── main.py                # FastAPI entry point
│   ├── api/
│   │   ├── __init__.py
│   │   └── endpoints.py       # REST API endpoints
│   ├── core/
│   │   ├── __init__.py
│   │   ├── parser.py          # Qlik script parser
│   │   ├── transformer.py     # AST to IR transformer
│   │   ├── codegen.py         # PySpark code generator
│   │   └── semantic.py        # Semantic model generator
│   ├── models/
│   │   ├── __init__.py
│   │   ├── ast_models.py      # AST node definitions
│   │   ├── ir_models.py       # Internal representation
│   │   └── api_models.py      # API request/response models
│   └── utils/
│       ├── __init__.py
│       └── qlik_functions.py  # Qlik→PySpark function mappings
│
├── tests/                      # Unit tests
│   ├── __init__.py
│   ├── test_parser.py
│   ├── test_transformer.py
│   ├── test_codegen.py
│   └── test_semantic.py
│
├── examples/                   # Sample files
│   ├── README.md
│   ├── sample_script.qvs      # Example Qlik script
│   └── sample_output.py       # Example PySpark output
│
├── venv/                       # Virtual environment (not in git)
├── requirements.txt            # Python dependencies
├── README.md                   # Main documentation
├── .gitignore                  # Git ignore rules
└── PROJECT_STRUCTURE.md        # This file
```

## Key Files

### Application Core
- `app/main.py` - FastAPI application initialization
- `app/api/endpoints.py` - `/convert` and `/health` endpoints
- `app/core/parser.py` - Parses Qlik script → AST
- `app/core/transformer.py` - Transforms AST → Internal Representation
- `app/core/codegen.py` - Generates PySpark code from IR
- `app/core/semantic.py` - Generates semantic model JSON

### Data Models
- `app/models/ast_models.py` - LoadStatement, FieldExpression, etc.
- `app/models/ir_models.py` - DataModel, TableDefinition, etc.
- `app/models/api_models.py` - ConvertRequest, ConvertResponse

### Utilities
- `app/utils/qlik_functions.py` - QlikFunctionMapper class

## Installation

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Running

### Start API Server
```bash
uvicorn app.main:app --reload
```

### Run Tests
```bash
pytest tests/ -v
```

## API Usage

```bash
POST http://localhost:8000/api/v1/convert
Content-Type: application/json

{
  "script": "LOAD CustomerID, CustomerName FROM customers.csv;",
  "mode": "transformation",
  "options": {
    "fabric_compatible": true
  }
}
```

## Features

✓ Parses Qlik LOAD statements (external, resident, inline, mapping)
✓ Handles JOINs (LEFT, RIGHT, INNER, OUTER)
✓ Supports WHERE, GROUP BY, DISTINCT
✓ Converts 50+ Qlik functions to PySpark equivalents
✓ Auto-detects table relationships
✓ Generates execution order based on dependencies
✓ Produces Microsoft Fabric-compatible code
✓ Creates semantic model JSON for Power BI/Fabric

## Version

1.0.0 - Production Ready

