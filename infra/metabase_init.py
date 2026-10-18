import json, urllib.request

MB = "http://metabase:3000"
ADMIN = {"email": "admin@example.com", "password": "Hw3-bigdata-2026!"}
TITLE = "ДЗ 3: CSV против Parquet с партиционированием"


def api(method, path, body=None, session=None):
    req = urllib.request.Request(MB + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json",
                                          **({"X-Metabase-Session": session} if session else {})})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read() or "null")


props = api("GET", "/api/session/properties")
if props.get("setup-token") and not props.get("has-user-setup"):
    session = api("POST", "/api/setup", {"token": props["setup-token"],
                                         "user": {**ADMIN, "first_name": "HW3", "last_name": "Admin", "site_name": "HW3"},
                                         "prefs": {"site_name": "HW3", "allow_tracking": False}})["id"]
else:
    session = api("POST", "/api/session", {"username": ADMIN["email"], "password": ADMIN["password"]})["id"]

existing = [d["id"] for d in api("GET", "/api/dashboard", session=session) if d["name"] == TITLE]
if existing:
    print(f"dashboard: http://localhost:3000/dashboard/{existing[0]}")
    raise SystemExit

api("PUT", "/api/setting/site-locale", {"value": "ru"}, session)
api("PUT", "/api/setting/custom-formatting", {"value": {"type/Number": {"number_separators": ", "}}}, session)

db = next((d["id"] for d in api("GET", "/api/database", session=session)["data"] if d["name"] == "hw3"), None)
if db is None:
    db = api("POST", "/api/database", {"engine": "postgres", "name": "hw3", "details": {
        "host": "postgres", "port": 5432, "dbname": "hw3", "user": "hw3", "password": "hw3"}}, session)["id"]


def card(name, sql, display, settings):
    return api("POST", "/api/card", {"name": name, "display": display, "visualization_settings": settings,
                                     "dataset_query": {"type": "native", "database": db, "native": {"query": sql}}},
               session)["id"]


bars = lambda metric, **extra: {"graph.dimensions": ["Запрос", "Вариант"], "graph.metrics": [metric],
                                "graph.show_values": True, "graph.label_value_frequency": "all", **extra}
cards = [
    (card("Время выполнения запроса, с (медиана 5 прогонов)",
          'SELECT "Запрос", "Вариант", "Время, с" FROM bench_v ORDER BY 1, 2', "bar", bars("Время, с")),
     dict(row=0, col=0, size_x=12, size_y=8)),
    (card("Прочитано с диска, МБ (Spark UI: Input), логарифмическая шкала",
          'SELECT "Запрос", "Вариант", "Прочитано, МБ" FROM bench_v ORDER BY 1, 2', "bar",
          bars("Прочитано, МБ", **{"graph.y_axis.scale": "log",
                                   "series_settings": {"CSV + дата": {"show_series_values": False}}})),
     dict(row=0, col=12, size_x=12, size_y=8)),
    (card("Размер датасета на диске, МБ",
          'SELECT split_part(format, \' \', 1) AS "Формат", size_mb AS "Размер, МБ" FROM storage ORDER BY size_mb DESC', "bar",
          {"graph.dimensions": ["Формат"], "graph.metrics": ["Размер, МБ"], "graph.show_values": True}),
     dict(row=8, col=0, size_x=8, size_y=7)),
    (card("Q1: выручка такси Нью-Йорка по дням, март 2024, $",
          'SELECT pickup_date AS "Дата", revenue AS "Выручка, $" FROM q1 ORDER BY 1', "line",
          {"graph.dimensions": ["Дата"], "graph.metrics": ["Выручка, $"]}),
     dict(row=8, col=8, size_x=16, size_y=7)),
]

dash = api("POST", "/api/dashboard", {"name": TITLE}, session)["id"]
api("PUT", f"/api/dashboard/{dash}", {"dashcards": [
    {"id": -i - 1, "card_id": c, "parameter_mappings": [], "visualization_settings": {}, **pos}
    for i, (c, pos) in enumerate(cards)]}, session)
print(f"dashboard: http://localhost:3000/dashboard/{dash}")
