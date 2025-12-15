
import pytest
from app.core.parser import QlikParser
from app.models.ast_models import Script, LoadStatement, LoadType, VariableAssignment

class TestQlikParser:

    def test_simple_load(self):

        script = "LOAD CustomerID, CustomerName FROM customers.csv;"
        parser = QlikParser()
        ast = parser.parse(script)

        assert isinstance(ast, Script)
        assert len(ast.statements) == 1
        assert isinstance(ast.statements[0], LoadStatement)

        load_stmt = ast.statements[0]
        assert load_stmt.load_type == LoadType.EXTERNAL
        assert len(load_stmt.fields) == 2
        assert load_stmt.fields[0].raw_expression == "CustomerID"
        assert load_stmt.fields[1].raw_expression == "CustomerName"

    def test_resident_load(self):

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

        assert len(ast.statements) == 2

        resident_load = ast.statements[1]
        assert resident_load.load_type == LoadType.RESIDENT
        assert resident_load.source == "Sales"
        assert resident_load.where_clause is not None

    def test_join_statement(self):

        script = """
        Orders:
        LOAD OrderID, CustomerID FROM orders.csv;

        LEFT JOIN (Orders)
        LOAD CustomerID, CustomerName FROM customers.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        assert len(ast.statements) == 2
        join_stmt = ast.statements[1]
        assert join_stmt.join_clause is not None
        assert join_stmt.join_clause.join_type.value == "left"

    def test_variable_assignment(self):

        script = """
        LET vToday = Today();
        SET vPath = 'C:\\Data\\';
        """
        parser = QlikParser()
        ast = parser.parse(script)

        assert len(ast.statements) == 2
        assert isinstance(ast.statements[0], VariableAssignment)
        assert ast.statements[0].variable_name == "vToday"
        assert ast.statements[0].is_let == True

        assert isinstance(ast.statements[1], VariableAssignment)
        assert ast.statements[1].is_let == False

    def test_mapping_load(self):

        script = """
        CountryMap:
        MAPPING LOAD
        CountryCode,
        CountryName
        FROM countries.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        assert len(ast.statements) == 1

    def test_inline_load(self):

        script = """
        Categories:
        LOAD * INLINE [
        CategoryID, CategoryName
        1, Electronics
        2, Clothing
        3, Food
        ];
        """
        parser = QlikParser()
        ast = parser.parse(script)

        assert len(ast.statements) == 1
        load_stmt = ast.statements[0]
        assert load_stmt.load_type == LoadType.INLINE
        assert load_stmt.inline_data is not None

    def test_group_by(self):

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

        assert len(ast.statements) == 1
        load_stmt = ast.statements[0]
        assert load_stmt.group_by is not None
        assert "CustomerID" in load_stmt.group_by.fields

    def test_distinct_load(self):

        script = """
        LOAD DISTINCT CustomerID FROM orders.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        assert len(ast.statements) == 1
        assert ast.statements[0].distinct == True

    def test_calculated_fields(self):

        script = """
        LOAD 
        OrderID,
        Year(OrderDate) as OrderYear,
        Amount * 1.1 as AmountWithTax
        FROM orders.csv;
        """
        parser = QlikParser()
        ast = parser.parse(script)

        assert len(ast.statements) == 1
        load_stmt = ast.statements[0]
        assert len(load_stmt.fields) == 3

        year_field = load_stmt.fields[1]
        assert year_field.alias == "OrderYear"
        assert year_field.is_calculated == True
