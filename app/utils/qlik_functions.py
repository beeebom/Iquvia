
from typing import Dict, Callable, List

class QlikFunctionMapper:

    FUNCTION_MAP: Dict[str, str] = {

        "Date": "to_date",
        "Today": "current_date",
        "Now": "current_timestamp",
        "Year": "year",
        "Month": "month",
        "Day": "dayofmonth",
        "Hour": "hour",
        "Minute": "minute",
        "Second": "second",
        "WeekDay": "dayofweek",
        "Week": "weekofyear",
        "Quarter": "quarter",

        "Upper": "upper",
        "Lower": "lower",
        "Trim": "trim",
        "LTrim": "ltrim",
        "RTrim": "rtrim",
        "Len": "length",
        "SubField": "split",
        "Left": "substring",
        "Right": "substring",
        "Mid": "substring",
        "Replace": "regexp_replace",
        "Capitalize": "initcap",
        "Concat": "concat",

        "Round": "round",
        "Floor": "floor",
        "Ceil": "ceil",
        "Abs": "abs",
        "Sqrt": "sqrt",
        "Pow": "pow",
        "Exp": "exp",
        "Log": "log",
        "Log10": "log10",
        "Mod": "mod",

        "Sum": "sum",
        "Count": "count",
        "Avg": "avg",
        "Min": "min",
        "Max": "max",
        "FirstValue": "first",
        "LastValue": "last",

        "If": "when",
        "Null": "lit(None)",
        "IsNull": "isnull",
        "Alt": "coalesce",

        "Num": "cast({}, 'double')",
        "Text": "cast({}, 'string')",
        "Dual": "{}",  
    }

    SPECIAL_FUNCTIONS = {
        "AddMonths", "AddYears", "MonthStart", "MonthEnd",
        "YearStart", "YearEnd", "WeekStart", "WeekEnd",
        "MakeDate", "Timestamp", "Date#", "Timestamp#",
        "SubField", "TextBetween", "MapSubString",
        "If", "Pick", "Match", "WildMatch",
        "ApplyMap", "Lookup"
    }

    @classmethod
    def map_function(cls, qlik_func: str, args: List[str]) -> str:

        func_upper = qlik_func.strip()

        if func_upper in cls.SPECIAL_FUNCTIONS:
            return cls._handle_special_function(func_upper, args)

        if func_upper in cls.FUNCTION_MAP:
            pyspark_func = cls.FUNCTION_MAP[func_upper]

            if "{}" in pyspark_func:
                return pyspark_func.format(args[0] if args else "")

            args_str = ", ".join(args)
            return f"{pyspark_func}({args_str})"

        args_str = ", ".join(args)
        return f"{qlik_func}({args_str})"

    @classmethod
    def _handle_special_function(cls, func: str, args: List[str]) -> str:

        if func == "AddMonths":

            return f"add_months({args[0]}, {args[1]})" if len(args) >= 2 else f"add_months({args[0]}, 0)"

        elif func == "AddYears":

            return f"add_months({args[0]}, {args[1]}*12)" if len(args) >= 2 else f"add_months({args[0]}, 0)"

        elif func == "MonthStart":

            return f"trunc({args[0]}, 'month')" if args else "trunc(current_date(), 'month')"

        elif func == "MonthEnd":

            return f"last_day({args[0]})" if args else "last_day(current_date())"

        elif func == "YearStart":

            return f"trunc({args[0]}, 'year')" if args else "trunc(current_date(), 'year')"

        elif func == "MakeDate":

            return f"make_date({', '.join(args[:3])})"

        elif func == "SubField":

            if len(args) >= 3:
                return f"split({args[0]}, {args[1]})[{args[2]}-1]"
            elif len(args) == 2:
                return f"split({args[0]}, {args[1]})"
            else:
                return f"split({args[0]}, ',')"

        elif func == "If":

            if len(args) >= 3:
                return f"when({args[0]}, {args[1]}).otherwise({args[2]})"
            elif len(args) == 2:
                return f"when({args[0]}, {args[1]}).otherwise(None)"
            else:
                return f"when({args[0]}, True).otherwise(False)"

        elif func == "ApplyMap":

            return f"# ApplyMap({', '.join(args)})"

        elif func == "Lookup":

            return f"# Lookup({', '.join(args)})"

        elif func in ["Date#", "Timestamp#"]:

            format_map = {
                "YYYY-MM-DD": "yyyy-MM-dd",
                "MM/DD/YYYY": "MM/dd/yyyy",
                "DD/MM/YYYY": "dd/MM/yyyy",
            }
            if len(args) >= 2:
                qlik_format = args[1].strip("'\"")
                pyspark_format = format_map.get(qlik_format, qlik_format)
                return f"to_date({args[0]}, '{pyspark_format}')"
            else:
                return f"to_date({args[0]})"

        return f"{func}({', '.join(args)})"

    @classmethod
    def is_aggregate_function(cls, func_name: str) -> bool:

        aggregate_funcs = {"Sum", "Count", "Avg", "Min", "Max", "FirstValue", "LastValue"}
        return func_name in aggregate_funcs
