
import pytest
from app.core.parser import QlikParser
from app.core.transformer import ASTTransformer
from app.models.ir_models import DataModel, SelectTransformation, FilterTransformation

class TestASTTransformer:

    def test_simple_transform(self):

        script = "LOAD CustomerID, CustomerName FROM customers.csv;"
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        assert isinstance(data_model, DataModel)
        assert len(data_model.tables) == 1

        table_name = list(data_model.tables.keys())[0]
        table = data_model.tables[table_name]
        assert table.source_type == "external"
        assert table.source_path == "customers.csv"
        assert len(table.columns) == 2

    def test_resident_transform(self):

        script = """
        Sales:
        LOAD * FROM sales.csv;

        FilteredSales:
        LOAD OrderID, Amount 
        RESIDENT Sales
        WHERE Amount > 100;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        assert len(data_model.tables) == 2

        assert len(data_model.execution_order) == 2
        assert data_model.execution_order[0] == "Sales"
        assert data_model.execution_order[1] == "FilteredSales"

        filtered_table = data_model.tables["FilteredSales"]
        assert any(isinstance(t, SelectTransformation) for t in filtered_table.transformations)
        assert any(isinstance(t, FilterTransformation) for t in filtered_table.transformations)

    def test_relationship_detection(self):

        script = """
        Orders:
        LOAD OrderID, CustomerID, Amount FROM orders.csv;

        Customers:
        LOAD CustomerID, CustomerName FROM customers.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        assert len(data_model.relationships) > 0
        rel = data_model.relationships[0]
        assert "CustomerID" in rel.from_columns or "CustomerID" in rel.to_columns

    def test_variable_storage(self):

        script = """
        LET vYear = 2024;
        SET vPath = 'data/';
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        assert "vYear" in data_model.variables
        assert "vPath" in data_model.variables

    def test_mapping_storage(self):

        script = """
        CountryMap:
        MAPPING LOAD
        CountryCode,
        CountryName
        FROM countries.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        transformer = ASTTransformer()
        data_model = transformer.transform(ast)

        assert "CountryMap" in data_model.mappings
        mapping = data_model.mappings["CountryMap"]
        assert mapping.key_column == "CountryCode"
        assert mapping.value_column == "CountryName"
