# Qlik-to-PySpark Converter

A backend service that converts Qlik Data Load Scripts (.qvs or .txt) into PySpark DataFrame code compatible with Microsoft Fabric, along with a semantic model JSON.

## Features

- **Parse Qlik Scripts**: Supports LOAD, RESIDENT LOAD, INLINE LOAD, MAPPING LOAD
- **Handle Transformations**: JOINs, WHERE, GROUP BY, DISTINCT, ApplyMap, variables, nested transformations
- **Function Mapping**: Converts Qlik functions (Date, Year, Month, If, Upper, Lower, etc.) to PySpark equivalents
- **Auto-detect Relationships**: Identifies shared key fields for JOIN logic
- **Execution Planning**: Builds dependency chain for script execution
- **Dual Mode**: Supports both "Extraction" and "Transformation" modes
- **Semantic Model**: Generates JSON describing tables, columns, data types, and relationships

## Project Structure

```
iquvia/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application entry point
│   ├── api/
│   │   ├── __init__.py
│   │   └── endpoints.py        # API endpoints
│   ├── core/
│   │   ├── __init__.py
│   │   ├── parser.py           # Qlik script parser
│   │   ├── transformer.py      # AST to internal representation
│   │   ├── codegen.py          # PySpark code generator
│   │   └── semantic.py         # Semantic model generator
│   ├── models/
│   │   ├── __init__.py
│   │   ├── ast_models.py       # AST node definitions
│   │   ├── ir_models.py        # Internal representation models
│   │   └── api_models.py       # Request/Response models
│   └── utils/
│       ├── __init__.py
│       └── qlik_functions.py   # Qlik to PySpark function mappings
├── tests/
│   ├── __init__.py
│   ├── test_parser.py
│   ├── test_transformer.py
│   ├── test_codegen.py
│   └── test_semantic.py
├── examples/
│   ├── sample_script.qvs
│   └── sample_output.py
├── requirements.txt
└── README.md
```

## Installation

1. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Start the FastAPI server:
```bash
uvicorn app.main:app --reload
```

2. Access the API documentation:
```
http://localhost:8000/docs
```

3. Upload a Qlik script via the `/convert` endpoint

## API Endpoints

### POST /convert
Convert a Qlik script to PySpark code and semantic model.

**Request Body:**
```json
{
  "script": "LOAD * FROM data.csv;",
  "mode": "transformation",
  "options": {
    "fabric_compatible": true
  }
}
```

**Response:**
```json
{
  "pyspark_code": "df = spark.read.csv('data.csv', header=True, inferSchema=True)",
  "semantic_model": {...},
  "execution_plan": [...]
}
```

## Testing

Run tests:
```bash
pytest tests/
```

## License

MIT

