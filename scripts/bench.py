import csv, json, math, os, statistics, time, urllib.request
from datetime import date, timedelta
from pyspark.sql import SparkSession

RUNS = 5

spark = (SparkSession.builder.master("local[*]").appName("bench")
         .config("spark.driver.memory", "6g")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
API = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"

schema = spark.read.parquet("data/parquet").schema
spark.read.schema(schema).option("header", True).csv("data/csv").createOrReplaceTempView("trips_csv")
spark.read.parquet("data/parquet").createOrReplaceTempView("trips_parquet")
spark.read.option("header", True).option("inferSchema", True) \
    .csv("data/raw/taxi_zone_lookup.csv").cache().createOrReplaceTempView("zones")
spark.table("zones").count()

QUERIES = {
    "q1": dict(
        title="Поездки и выручка по дням, март 2024",
        dates=("2024-03-01", "2024-03-31"), where="", cols="pickup_date, total_amount, trip_distance",
        sql="""SELECT pickup_date, count(*) AS trips, round(sum(total_amount), 2) AS revenue,
                      round(avg(trip_distance), 2) AS avg_distance
               FROM {src} GROUP BY pickup_date ORDER BY pickup_date"""),
    "q2": dict(
        title="Процент чаевых по часу и способу оплаты, неделя 13-19 мая",
        dates=("2024-05-13", "2024-05-19"), where="AND fare_amount > 0",
        cols="tpep_pickup_datetime, payment_type, tip_amount, fare_amount",
        sql="""SELECT hour(tpep_pickup_datetime) AS hour, payment_type, count(*) AS trips,
                      round(avg(tip_amount / fare_amount) * 100, 2) AS tip_pct
               FROM {src} GROUP BY 1, 2 ORDER BY 1, 2"""),
    "q3": dict(
        title="Топ-10 зон посадки по выручке, II квартал 2024",
        dates=("2024-04-01", "2024-06-30"), where="AND trip_distance > 0",
        cols="PULocationID, total_amount, trip_distance",
        sql="""SELECT z.Borough, z.Zone, count(*) AS trips, round(sum(t.total_amount), 2) AS revenue
               FROM {src} JOIN zones z ON t.PULocationID = z.LocationID
               GROUP BY 1, 2 ORDER BY revenue DESC LIMIT 10"""),
}


def ts_range(a, b):
    end = date.fromisoformat(b) + timedelta(days=1)
    return f"tpep_pickup_datetime >= TIMESTAMP'{a} 00:00:00' AND tpep_pickup_datetime < TIMESTAMP'{end} 00:00:00'"


def source(q, storage, variant):
    a, b = q["dates"]
    if variant == "naive":
        return f"(SELECT * FROM trips_{storage} WHERE {ts_range(a, b)} {q['where']}) t"
    return (f"(SELECT {q['cols']} FROM trips_{storage} WHERE pickup_date "
            f"BETWEEN DATE'{a}' AND DATE'{b}' {q['where']}) t")


def get(path):
    return json.load(urllib.request.urlopen(API + path))


def metrics(tag):
    for _ in range(50):
        ex = [e for e in get("/sql?details=true&planDescription=false&length=100000") if e["description"] == tag]
        if ex and ex[0]["status"] == "COMPLETED" and not ex[0]["runningJobIds"]:
            break
        time.sleep(0.1)
    ex = ex[0]
    stages = {s for j in get("/jobs") if j["jobId"] in ex["successJobIds"] for s in j["stageIds"]}
    input_bytes = sum(a["inputBytes"] for s in stages for a in get(f"/stages/{s}") if a["status"] == "COMPLETE")
    scan = {}
    for n in ex["nodes"]:
        if n["nodeName"] in ("Scan csv", "Scan parquet"):
            scan = {m["name"]: m["value"] for m in n["metrics"]}
    return dict(input_mb=round(input_bytes / 2**20, 1),
                files_read=int(scan.get("number of files read", "0").replace(",", "")),
                partitions_read=scan.get("number of partitions read", "-"),
                size_of_files=scan.get("size of files read", "-"))


def measure(tag, action):
    times = []
    for i in range(RUNS + 1):
        spark._jvm.System.gc()
        sc.setJobDescription(f"{tag}|{i}")
        t = time.perf_counter()
        out = action()
        times.append(time.perf_counter() - t)
        sc.setJobDescription(None)
    return dict(median_s=round(statistics.median(times[1:]), 3),
                runs_s=" / ".join(f"{t:.3f}" for t in times[1:]), **metrics(f"{tag}|{RUNS}")), out


def same(r1, r2):
    if len(r1) != len(r2):
        return False
    for a, b in zip(r1, r2):
        for x, y in zip(a, b):
            if isinstance(x, float):
                if not math.isclose(x, y, rel_tol=1e-9, abs_tol=0.011):
                    return False
            elif x != y:
                return False
    return True


def save_plan(name, sql):
    with open(f"results/plans/{name}.txt", "w") as f:
        f.write(sql.strip() + "\n\n" + spark.sql("EXPLAIN FORMATTED " + sql).collect()[0][0])


def write_csv(name, rows):
    with open(f"results/{name}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


def data_files(path, ext):
    return [os.path.join(d, f) for d, _, fs in os.walk(path) for f in fs if f.endswith(ext)]


os.makedirs("results/plans", exist_ok=True)

for q in QUERIES.values():
    for storage in ("csv", "parquet"):
        for variant in ("naive", "pruned"):
            spark.sql(q["sql"].format(src=source(q, storage, variant))).collect()

summary = []
for qid, q in QUERIES.items():
    reference = None
    for storage in ("csv", "parquet"):
        for variant in ("naive", "pruned"):
            sql = q["sql"].format(src=source(q, storage, variant))
            save_plan(f"{qid}_{storage}_{variant}", sql)
            m, rows = measure(f"{qid}|{storage}|{variant}", lambda: spark.sql(sql).collect())
            if reference is None:
                reference = rows
                write_csv(qid, [r.asDict() for r in rows])
            assert same(rows, reference), f"{qid} {storage}/{variant}: результат отличается"
            summary.append(dict(query=qid, storage=storage, variant=variant, **m))
            print(summary[-1], flush=True)
write_csv("bench", summary)

pushdown = []
sql = QUERIES["q2"]["sql"].format(src=source(QUERIES["q2"], "parquet", "naive"))
for enabled in ("false", "true"):
    spark.conf.set("spark.sql.parquet.filterPushdown", enabled)
    save_plan(f"pushdown_{enabled}", sql)
    m, _ = measure(f"pushdown|{enabled}", lambda: spark.sql(sql).collect())
    pushdown.append(dict(filter_pushdown=enabled, **m))
    print(pushdown[-1], flush=True)
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
write_csv("pushdown", pushdown)

projection = []
for storage in ("csv", "parquet"):
    for cols in ("*", QUERIES["q1"]["cols"]):
        sql = (f"SELECT {cols} FROM trips_{storage} "
               f"WHERE pickup_date BETWEEN DATE'2024-03-01' AND DATE'2024-03-31'")
        df = spark.sql(sql)
        m, _ = measure(f"proj|{storage}|{cols}", lambda: df.write.format("noop").mode("overwrite").save())
        projection.append(dict(storage=storage, columns="все 20" if cols == "*" else cols, **m))
        print(projection[-1], flush=True)
write_csv("projection", projection)

storage_rows = [dict(format=fmt, files=len(fs), size_mb=round(sum(map(os.path.getsize, fs)) / 2**20, 1))
                for fmt, fs in (("CSV", data_files("data/csv", ".csv")),
                                ("Parquet (snappy, по дате)", data_files("data/parquet", ".parquet")))]
write_csv("storage", storage_rows)
print(storage_rows)
