
import pytest
from app.core.parser import QlikParser
from app.core.transformer import ASTTransformer
from app.core.semantic import SemanticModelGenerator

class TestSemanticModelGenerator:

    def test_basic_semantic_model(self):

        script = "LOAD CustomerID, CustomerName FROM customers.csv;"
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        semantic_gen = SemanticModelGenerator()
        semantic_model = semantic_gen.generate(data_model)

        assert "tables" in semantic_model
        assert "relationships" in semantic_model
        assert len(semantic_model["tables"]) == 1

        table = semantic_model["tables"][0]
        assert "name" in table
        assert "columns" in table
        assert len(table["columns"]) > 0

    def test_relationships_in_semantic_model(self):

        script = """
        Orders:
        LOAD OrderID, CustomerID FROM orders.csv;

        Customers:
        LOAD CustomerID, CustomerName FROM customers.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        semantic_gen = SemanticModelGenerator()
        semantic_model = semantic_gen.generate(data_model)

        if len(data_model.relationships) > 0:
            assert len(semantic_model["relationships"]) > 0
            rel = semantic_model["relationships"][0]
            assert "fromTable" in rel
            assert "toTable" in rel
            assert "cardinality" in rel

    def test_column_metadata(self):

        script = """
        Sales:
        LOAD 
        OrderID,
        Year(OrderDate) as OrderYear
        FROM sales.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        semantic_gen = SemanticModelGenerator()
        semantic_model = semantic_gen.generate(data_model)

        table = semantic_model["tables"][0]
        for col in table["columns"]:
            assert "dataType" in col
            assert "nullable" in col

    def test_measures_generation(self):

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

        semantic_gen = SemanticModelGenerator()
        semantic_model = semantic_gen.generate(data_model)

        assert "measures" in semantic_model
