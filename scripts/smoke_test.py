import requests

API="http://localhost/api/v1/search"

tests = [
    "bmw",
    "toyota camry",
    "мерседес glc",
    "bmw x5 до 3 млн",
    "toyota prado дизель"
]

for q in tests:

    r = requests.post(API,json={"query":q})

    data = r.json()

    print("\nQUERY:",q)
    print("RESULTS:",len(data["results"]))

    if len(data["results"]) == 0:
        print("ERROR: empty results")
