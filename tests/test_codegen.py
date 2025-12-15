
import pytest
from app.core.parser import QlikParser
from app.core.transformer import ASTTransformer
from app.core.codegen import PySparkCodeGenerator

class TestPySparkCodeGenerator:

    def test_simple_code_generation(self):

        script = "LOAD CustomerID, CustomerName FROM customers.csv;"
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        codegen = PySparkCodeGenerator()
        code = codegen.generate(data_model)

        assert "spark.read.csv" in code
        assert "customers.csv" in code
        assert "from pyspark.sql import SparkSession" in code

    def test_fabric_compatible_mode(self):

        script = "LOAD * FROM data.csv;"
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        codegen = PySparkCodeGenerator(fabric_compatible=True)
        code = codegen.generate(data_model)

        assert "Microsoft Fabric Compatible" in code

    def test_filter_generation(self):

        script = """
        Sales:
        LOAD * FROM sales.csv
        WHERE Amount > 100;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        codegen = PySparkCodeGenerator()
        code = codegen.generate(data_model)

        assert ".filter(" in code

    def test_join_generation(self):

        script = """
        Orders:
        LOAD OrderID, CustomerID FROM orders.csv;

        LEFT JOIN (Orders)
        LOAD CustomerID, CustomerName FROM customers.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        codegen = PySparkCodeGenerator()
        code = codegen.generate(data_model)

        assert ".join(" in code
        assert "'left'" in code or "left" in code

    def test_aggregation_generation(self):

        script = """
        SalesSummary:
        LOAD 
        CustomerID,
        Sum(Amount) as TotalAmount
        RESIDENT Sales
        GROUP BY CustomerID;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        codegen = PySparkCodeGenerator()
        code = codegen.generate(data_model)

        assert ".groupBy(" in code
        assert ".agg(" in code

    def test_distinct_generation(self):

        script = "LOAD DISTINCT CustomerID FROM orders.csv;"
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        codegen = PySparkCodeGenerator()
        code = codegen.generate(data_model)

        assert ".distinct()" in code
