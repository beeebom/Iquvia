import re
import sys
import json
from pathlib import Path

class QlikConverter:
    def __init__(self):
        self.variables = {}
        self.mappings = {}
        self.table_count = 0
        self.tables_metadata = {}
        self.current_table = None
        
    def convert(self, qlik_script: str):
        statements = self._parse_statements(qlik_script)
        
        code = [
            "from pyspark.sql.functions import *",
            "from pyspark.sql.types import *",
            ""
        ]
        
        for stmt in statements:
            stmt_expanded = self._expand_vars(stmt)
            
            if stmt.upper().strip().startswith(('LET ', 'SET ')):
                code.extend(self._convert_variable(stmt))
            elif 'MAPPING LOAD' in stmt.upper():
                code.extend(self._convert_mapping(stmt_expanded))
            elif 'LOAD' in stmt.upper():
                code.extend(self._convert_load(stmt_expanded))
        
        semantic = self._build_semantic_model()
        
        return "\n".join(code), semantic
    
    def _build_semantic_model(self):
        tables_list = []
        relationships_list = []
        
        for tbl_name, tbl_data in self.tables_metadata.items():
            table_entry = {
                "name": tbl_name,
                "columns": tbl_data.get("columns", [])
            }
            if tbl_data.get("measures"):
                table_entry["measures"] = tbl_data["measures"]
            tables_list.append(table_entry)
        
        all_tables = list(self.tables_metadata.keys())
        for i, tbl1 in enumerate(all_tables):
            for tbl2 in all_tables[i+1:]:
                cols1 = {c["name"] for c in self.tables_metadata[tbl1].get("columns", [])}
                cols2 = {c["name"] for c in self.tables_metadata[tbl2].get("columns", [])}
                common = cols1 & cols2
                if common:
                    for col_name in common:
                        relationships_list.append({
                            "name": f"Rel_{tbl1}_{tbl2}",
                            "fromTable": tbl1,
                            "fromColumn": col_name,
                            "toTable": tbl2,
                            "toColumn": col_name
                        })
        
        return {
            "name": "QlikConvertedModel",
            "compatibilityLevel": 1600,
            "model": {
                "culture": "en-US",
                "tables": tables_list,
                "relationships": relationships_list
            }
        }
    
    def _parse_statements(self, script):
        script = re.sub(r'//.*', '', script)
        stmts, curr, in_inline = [], "", False
        for line in script.split('\n'):
            line_stripped = line.strip()
            
            if 'INLINE [' in line_stripped.upper():
                in_inline = True
            
            if in_inline:
                curr += "\n" + line_stripped
            else:
                curr += " " + line_stripped
            
            if line_stripped.endswith('];') or (line_stripped.endswith(';') and not in_inline):
                stmts.append(curr.strip())
                curr = ""
                in_inline = False
        if curr.strip():
            stmts.append(curr.strip())
        return stmts
    
    def _expand_vars(self, text):
        for name, val in self.variables.items():
            text = text.replace(f"$({name})", val.strip("'\""))
        return text
    
    def _convert_variable(self, stmt):
        m = re.match(r'(LET|SET)\s+(\w+)\s*=\s*(.+);?', stmt, re.I)
        if m:
            self.variables[m.group(2)] = m.group(3).rstrip(';').strip()
            if m.group(1).upper() == 'LET' and m.group(3).strip().isdigit():
                return [f"{m.group(2)} = {m.group(3).rstrip(';').strip()}", ""]
        return [""]
    
    def _convert_mapping(self, stmt):
        m_name = re.search(r'(\w+):\s*MAPPING', stmt, re.I)
        map_name = m_name.group(1) if m_name else "Map1"
        
        flds = re.search(r'MAPPING\s+LOAD\s+(.*?)\s+FROM', stmt, re.I|re.S)
        if flds:
            parts = [p.strip() for p in flds.group(1).split(',')]
            key_fld = parts[0] if len(parts) > 0 else "key"
            val_fld = parts[1] if len(parts) > 1 else "value"
        else:
            key_fld, val_fld = "key", "value"
        
        src = re.search(r'FROM\s+([\w\.\/]+)', stmt, re.I)
        source_file = src.group(1) if src else "data.csv"
        
        self.mappings[map_name] = (key_fld, val_fld)
        
        df = f"map_{map_name.lower()}"
        code = [
            f"{df} = spark.read.csv('{source_file}', header=True, inferSchema=True).select(col('{key_fld}').alias('_k'), col('{val_fld}').alias('_v'))"
        ]
        
        where = re.search(r'WHERE\s+(.*?);', stmt, re.I)
        if where:
            cond = where.group(1).strip().replace(' = ', ' == ')
            code.append(f"{df} = {df}.filter(col('{cond.split()[0]}') == {cond.split()[-1]})")
        
        code.append(f"{df} = broadcast({df})")
        code.append("")
        return code
    
    def _convert_load(self, stmt):
        self.table_count += 1
        
        tbl = re.match(r'(\w+):\s*', stmt, re.I)
        table_name = tbl.group(1) if tbl else f"Table{self.table_count}"
        df = f"df_{table_name.lower()}"
        
        self.current_table = table_name
        self.tables_metadata[table_name] = {"columns": [], "measures": []}
        
        code = [f"# Table: {table_name}"]
        
        if 'INLINE [' in stmt.upper():
            code.extend(self._inline_load(stmt, df, table_name))
        elif 'RESIDENT' in stmt.upper():
            code.extend(self._resident_load(stmt, df, table_name))
        elif re.search(r'(LEFT|RIGHT|INNER)?\s*JOIN', stmt, re.I):
            code.extend(self._join_load(stmt, df, table_name))
        else:
            code.extend(self._external_load(stmt, df, table_name))
        
        code.append("")
        return code
    
    def _external_load(self, stmt, df, table_name):
        src = re.search(r'FROM\s+([\[\]\.\/\w]+)', stmt, re.I)
        source = src.group(1).strip('[]') if src else "data.csv"
        
        code = [f"{df} = spark.read.csv('{source}', header=True, inferSchema=True)"]
        
        flds = re.search(r'LOAD(?:\s+DISTINCT)?\s+(.*?)\s+FROM', stmt, re.I|re.S)
        if flds and flds.group(1).strip() != '*':
            field_text = flds.group(1).strip()
            selects = self._parse_fields(field_text)
            self._track_columns(field_text, table_name)
            if selects:
                code.append(f"{df} = {df}.select({', '.join(selects)})")
        
        where = re.search(r'WHERE\s+(.*?)(?:;|$)', stmt, re.I)
        if where:
            cond = self._fix_condition(where.group(1))
            code.append(f"{df} = {df}.filter({cond})")
        
        if 'DISTINCT' in stmt.upper():
            code.append(f"{df} = {df}.distinct()")
        
        return code
    
    def _resident_load(self, stmt, df, table_name):
        res = re.search(r'RESIDENT\s+(\w+)', stmt, re.I)
        src_table = res.group(1) if res else "Table"
        src_df = f"df_{src_table.lower()}"
        
        code = [f"{df} = {src_df}"]
        
        flds = re.search(r'LOAD(?:\s+DISTINCT)?\s+(.*?)\s+RESIDENT', stmt, re.I|re.S)
        if flds and flds.group(1).strip() != '*' and 'GROUP BY' not in stmt.upper():
            field_text = flds.group(1).strip()
            selects = self._parse_fields(field_text)
            self._track_columns(field_text, table_name)
            if selects:
                code.append(f"{df} = {df}.select({', '.join(selects)})")
        elif 'GROUP BY' not in stmt.upper() and src_table in self.tables_metadata:
            self.tables_metadata[table_name]["columns"] = self.tables_metadata[src_table]["columns"].copy()
        
        if 'GROUP BY' in stmt.upper():
            code.extend(self._group_by(stmt, df, table_name))
        elif 'DISTINCT' in stmt.upper():
            code.append(f"{df} = {df}.distinct()")
        
        return code
    
    def _join_load(self, stmt, df, table_name):
        join_m = re.search(r'(LEFT|RIGHT|INNER)?\s*JOIN\s*\((\w+)\)', stmt, re.I)
        target_tbl = join_m.group(2) if join_m and join_m.group(2) else None
        join_type = join_m.group(1).lower() if join_m and join_m.group(1) else "inner"
        
        if not target_tbl:
            return [f"# JOIN (target unknown)"]
        
        target_df = f"df_{target_tbl.lower()}"
        temp = "df_join_right"
        
        src = re.search(r'FROM\s+([\[\]\.\/\w]+)', stmt, re.I)
        source = src.group(1).strip('[]') if src else "data.csv"
        
        code = [
            f"{temp} = spark.read.csv('{source}', header=True, inferSchema=True)"
        ]
        
        flds = re.search(r'LOAD\s+(.*?)\s+FROM', stmt, re.I|re.S)
        if flds and flds.group(1).strip() != '*':
            field_text = flds.group(1).strip()
            selects = self._parse_fields(field_text)
            if target_tbl in self.tables_metadata:
                for col_data in self._extract_column_info(field_text):
                    if col_data not in self.tables_metadata[target_tbl]["columns"]:
                        self.tables_metadata[target_tbl]["columns"].append(col_data)
            if selects:
                code.append(f"{temp} = {temp}.select({', '.join(selects)})")
        
        code.append(f"{target_df} = {target_df}.join({temp}, on=['OrderID'], how='{join_type}')")
        
        return code
    
    def _inline_load(self, stmt, df, table_name):
        inline = re.search(r'INLINE\s*\[(.*?)\]', stmt, re.I|re.S)
        if not inline:
            return []
        
        rows = [r.strip() for r in inline.group(1).strip().split('\n') if r.strip()]
        if not rows:
            return []
        
        if ',' in rows[0]:
            headers = [h.strip() for h in rows[0].split(',')]
            data = []
            for r in rows[1:]:
                vals = [v.strip() for v in r.split(',')]
                if len(vals) == len(headers):
                    data.append(tuple(vals))
        else:
            parts = rows[0].strip().split()
            headers = parts
            data = [tuple(r.split()) for r in rows[1:]]
        
        for h in headers:
            self.tables_metadata[table_name]["columns"].append({
                "name": h,
                "dataType": "string",
                "sourceColumn": h
            })
        
        schema_def = ', '.join([f"StructField('{h}', StringType(), True)" for h in headers])
        
        return [
            f"{df}_schema = StructType([{schema_def}])",
            f"{df}_data = {data}",
            f"{df} = spark.createDataFrame({df}_data, schema={df}_schema)"
        ]
    
    def _group_by(self, stmt, df, table_name):
        grp = re.search(r'GROUP BY\s+(.*?)(?:;|$)', stmt, re.I)
        if not grp:
            return []
        
        grp_cols = [c.strip() for c in grp.group(1).split(',')]
        
        flds = re.search(r'LOAD\s+(.*?)\s+RESIDENT', stmt, re.I|re.S)
        if not flds:
            return []
        
        for g in grp_cols:
            self.tables_metadata[table_name]["columns"].append({
                "name": g,
                "dataType": "string",
                "sourceColumn": g
            })
        
        aggs = []
        measures = []
        for fld in flds.group(1).split(','):
            fld = fld.strip()
            if any(a in fld.lower() for a in ['sum(', 'count(', 'avg(', 'min(', 'max(']):
                alias = re.search(r'as\s+(\w+)', fld, re.I)
                alias_name = alias.group(1) if alias else "agg"
                expr_part = fld[:alias.start()].strip() if alias else fld
                converted = self._fix_func(expr_part)
                aggs.append(f"{converted}.alias('{alias_name}')")
                
                measures.append({
                    "name": alias_name,
                    "expression": f"SUM({table_name}[Amount])" if 'sum' in fld.lower() else fld,
                    "formatString": "$#,0.00" if 'amount' in fld.lower() or 'total' in alias_name.lower() else "#,0"
                })
                
                self.tables_metadata[table_name]["columns"].append({
                    "name": alias_name,
                    "dataType": "double",
                    "sourceColumn": alias_name
                })
        
        self.tables_metadata[table_name]["measures"] = measures
        
        grp_str = ', '.join([f"col('{g}')" for g in grp_cols])
        agg_str = ', '.join(aggs)
        
        return [f"{df} = {df}.groupBy({grp_str}).agg({agg_str})"]
    
    def _parse_fields(self, text):
        fields = self._smart_split(text, ',')
        selects = []
        
        for fld in fields:
            fld = fld.strip()
            if not fld:
                continue
            
            if 'ApplyMap' in fld:
                alias = re.search(r'as\s+(\w+)', fld, re.I)
                alias_name = alias.group(1) if alias else "mapped"
                selects.append(f"lit('TODO_APPLYMAP').alias('{alias_name}')")
                continue
            
            alias = re.search(r'\s+as\s+(\w+)$', fld, re.I)
            if alias:
                alias_name = alias.group(1)
                expr = fld[:alias.start()].strip()
                conv = self._fix_func(expr)
                selects.append(f"{conv}.alias('{alias_name}')")
            elif '(' in fld or '*' in fld:
                conv = self._fix_func(fld)
                selects.append(conv)
            else:
                selects.append(f"col('{fld}')")
        
        return selects
    
    def _smart_split(self, text, delim):
        parts, curr, depth, in_q = [], "", 0, False
        for ch in text:
            if ch in ('"', "'") and (not in_q or ch == in_q):
                in_q = False if in_q else ch
            if ch == '(' and not in_q:
                depth += 1
            if ch == ')' and not in_q:
                depth -= 1
            if ch == delim and depth == 0 and not in_q:
                parts.append(curr)
                curr = ""
            else:
                curr += ch
        if curr:
            parts.append(curr)
        return parts
    
    def _fix_func(self, expr):
        funcs = {
            'Year': 'year', 'Month': 'month', 'Day': 'dayofmonth',
            'Upper': 'upper', 'Lower': 'lower', 'Trim': 'trim',
            'Sum': 'sum', 'Count': 'count', 'Avg': 'avg', 'Min': 'min', 'Max': 'max'
        }
        
        for qf, sf in funcs.items():
            expr = re.sub(rf'{qf}\((\w+)\)', rf"{sf}(col('\1'))", expr, flags=re.I)
        
        expr = re.sub(r'(\w+)\s*\*\s*([\d\.]+)', r"(col('\1') * \2)", expr)
        
        if not any(k in expr for k in ['col(', 'lit(', 'when(', 'sum(', 'count(', 'avg(', 'year(', 'month(', 'upper(', 'lower(']):
            if re.match(r'^\w+$', expr):
                expr = f"col('{expr}')"
        
        return expr
    
    def _fix_condition(self, cond):
        cond = self._fix_func(cond)
        cond = re.sub(r'(?<![=!<>])=(?!=)', '==', cond)
        return cond
    
    def _track_columns(self, fields_text, table_name):
        for col_info in self._extract_column_info(fields_text):
            self.tables_metadata[table_name]["columns"].append(col_info)
    
    def _extract_column_info(self, fields_text):
        columns = []
        fields = self._smart_split(fields_text, ',')
        
        for fld in fields:
            fld = fld.strip()
            if not fld or 'ApplyMap' in fld:
                continue
            
            alias = re.search(r'\s+as\s+(\w+)$', fld, re.I)
            col_name = alias.group(1) if alias else fld.strip()
            
            data_type = "string"
            if any(f in fld.lower() for f in ['year(', 'month(', 'day(', 'count(']):
                data_type = "int"
            elif any(f in fld.lower() for f in ['sum(', 'avg(', '*', '/']):
                data_type = "double"
            elif any(f in fld.lower() for f in ['date', 'timestamp']):
                data_type = "dateTime"
            
            columns.append({
                "name": col_name,
                "dataType": data_type,
                "sourceColumn": col_name
            })
        
        return columns

def main():
    if len(sys.argv) < 2:
        print("Qlik to PySpark Converter")
        print("\nUsage:")
        print("  python converter.py data/input.qvs")
        print("  python converter.py data/input.qvs data/output.py")
        print("\nOutput:")
        print("  - PySpark code (.py)")
        print("  - Semantic model (.json)")
        sys.exit(1)
    
    input_file = Path(sys.argv[1])
    if not input_file.exists():
        print(f"Error: {input_file} not found")
        sys.exit(1)
    
    output_file = Path(sys.argv[2]) if len(sys.argv) > 2 else input_file.parent / f"{input_file.stem}_pyspark.py"
    
    with open(input_file, 'r') as f:
        script = f.read()
    
    converter = QlikConverter()
    pyspark, semantic = converter.convert(script)
    
    output_file.write_text(pyspark)
    (output_file.parent / f"{output_file.stem}_semantic.json").write_text(json.dumps(semantic, indent=2))
    
    print(f"✓ Converted: {input_file.name}")
    print(f"  → {output_file}")
    print(f"  → {output_file.parent / f'{output_file.stem}_semantic.json'}")
    print(f"\n  Tables: {len(semantic['model']['tables'])}")
    print(f"  Relationships: {len(semantic['model']['relationships'])}")

if __name__ == "__main__":
    main()

