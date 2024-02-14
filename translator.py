# translator.py
import re

def _extract_select_expressions(plan_string):
    if "Project" in plan_string:
        start_idx = plan_string.find("[", plan_string.find("Project")) + 1
        end_idx = plan_string.find("]", start_idx)
        select_exprs = plan_string[start_idx:end_idx]

        # Split the select expressions into individual column references
        column_refs = select_exprs.split(',')

        # Clean each column reference and extract the column name
        cleaned_column_names = []
        for ref in column_refs:
            # Remove '#...' from each column reference
            cleaned_ref = re.sub(r'#\S+', '', ref).strip().replace("'","")

            cleaned_column_names.append(cleaned_ref)
            

        # Join the cleaned column names back into a comma-separated string
        formatted_select_exprs = ", ".join(cleaned_column_names)

        return formatted_select_exprs
    return "*"

def _extract_aggregate_functions(plan_string):
    if "Aggregate" in plan_string:
        start_idx = plan_string.find("[", plan_string.find("Aggregate")) + 1
        end_idx = plan_string.find("]", start_idx)
        aggregate_exprs = plan_string[start_idx:end_idx]

        # Additional parsing and formatting logic as needed
        # ...

        return aggregate_exprs
    return ""


def _extract_join_conditions(plan_string):
    if "Join" in plan_string:
        start_idx = plan_string.find("Join", plan_string.find("Join")) + 1
        end_idx = plan_string.find("]", start_idx)
        join_exprs = plan_string[start_idx:end_idx]

        # Additional parsing and formatting logic as needed
        # ...

        return join_exprs
    return ""


def _extract_filter_conditions(plan_string):
    if "Filter" in plan_string:
        # Locate the start and end of the filter condition
        filter_start = plan_string.find("Filter") + len("Filter")
        where_clause_start = plan_string.find("(", filter_start) + 1
        where_clause_end = plan_string.find(")", where_clause_start)

        filter_expr = plan_string[where_clause_start:where_clause_end].strip()

        # Clean the filter expression
        # Remove any internal Spark representation such as '#0L'
        clean_filter_expr = re.sub(r'#\S+', '', filter_expr)

        clean_filter_expr = clean_filter_expr.replace(":", "").replace("'Filter '","").replace("'","")

        end_of_expression = clean_filter_expr.find("\n")
        if end_of_expression != -1:
            clean_filter_expr = clean_filter_expr[:end_of_expression].strip()


        # Further parsing might be required depending on the complexity of the filter conditions
        # Split the expression by space and check if any part is a non-numeric string
        quoted_filter_expr = re.sub(r'(=\s*)(\w+)', r"\1'\2'", filter_expr)


        print(quoted_filter_expr)


        return " WHERE " + quoted_filter_expr
    return ""


def dataframe_to_sql(df):
    plan = df._jdf.queryExecution().logical()
    plan_string = plan.toString()

    # Start building the SQL query
    sql_query = "SELECT "

    # Extract various components of the SQL query
    sql_query += _extract_select_expressions(plan_string)
    sql_query += f" FROM table"
    sql_query += _extract_filter_conditions(plan_string)
    sql_query += _extract_aggregate_functions(plan_string)
    sql_query += _extract_join_conditions(plan_string)



    return sql_query
