import sys
import json
from app.core.parser import QlikParser
from app.core.transformer import ASTTransformer
from app.core.codegen import PySparkCodeGenerator
from app.core.semantic import SemanticModelGenerator

def convert_qlik_file(input_file, output_file=None):
    print(f"Reading Qlik script from: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        qlik_script = f.read()
    
    print("Parsing Qlik script...")
    parser = QlikParser()
    ast = parser.parse(qlik_script)
    print(f"  Parsed {len(ast.statements)} statements")
    
    print("Transforming to internal representation...")
    transformer = ASTTransformer()
    data_model = transformer.transform(ast)
    print(f"  Created {len(data_model.tables)} table(s)")
    print(f"  Execution order: {data_model.execution_order}")
    
    print("Generating PySpark code...")
    codegen = PySparkCodeGenerator(fabric_compatible=True)
    pyspark_code = codegen.generate(data_model, mode="transformation")
    
    print("Generating semantic model...")
    semantic_gen = SemanticModelGenerator()
    semantic_model = semantic_gen.generate(data_model)
    
    if output_file:
        py_output = output_file if output_file.endswith('.py') else f"{output_file}.py"
        json_output = output_file.replace('.py', '_semantic.json')
    else:
        py_output = input_file.replace('.qvs', '_output.py').replace('.txt', '_output.py')
        json_output = input_file.replace('.qvs', '_semantic.json').replace('.txt', '_semantic.json')
    
    with open(py_output, 'w', encoding='utf-8') as f:
        f.write(pyspark_code)
    print(f"\nPySpark code saved to: {py_output}")
    
    with open(json_output, 'w', encoding='utf-8') as f:
        json.dump(semantic_model, f, indent=2)
    print(f"Semantic model saved to: {json_output}")
    
    print("\n" + "="*60)
    print("CONVERSION COMPLETED SUCCESSFULLY!")
    print("="*60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python convert_qlik.py <input_file.qvs> [output_file.py]")
        print("\nExamples:")
        print("  python convert_qlik.py my_script.qvs")
        print("  python convert_qlik.py my_script.qvs my_output.py")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        convert_qlik_file(input_file, output_file)
    except FileNotFoundError:
        print(f"ERROR: File not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

