def union_queries(queries):
    union_all_sql = "\nUNION ALL\n"
    return union_all_sql.join(queries)
