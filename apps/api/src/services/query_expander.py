def expand_query(query):

    expansions = []

    q = query.lower()

    if "camry" in q:
        expansions += ["toyota camry", "camry xv70"]

    if "x5" in q:
        expansions += ["bmw x5", "bmw x5 f15"]

    return expansions