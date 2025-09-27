import sqlparse

def split_sql_statements(sql_text: str) -> tuple[str, str]:
    """
    Divide un script SQL en CREATE/DROP y el resto.
    Retorna tuple(create_sql, other_sql)
    """
    statements = sqlparse.split(sql_text)
    create_stmts = []
    other_stmts = []

    for stmt in statements:
        stmt_clean = stmt.strip()
        if not stmt_clean:
            continue
        if stmt_clean.upper().startswith(("CREATE", "DROP", "ALTER")):
            create_stmts.append(stmt_clean)
        else:
            other_stmts.append(stmt_clean)

    return "\n;\n".join(create_stmts) + ";", "\n;\n".join(other_stmts) + ";"
