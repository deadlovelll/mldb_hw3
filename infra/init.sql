CREATE EXTENSION file_fdw;
CREATE SERVER results FOREIGN DATA WRAPPER file_fdw;

CREATE FOREIGN TABLE bench (query text, storage text, variant text, median_s float, runs_s text,
                            input_mb float, files_read int, partitions_read text, size_of_files text)
    SERVER results OPTIONS (filename '/results/bench.csv', format 'csv', header 'true');
CREATE FOREIGN TABLE storage (format text, files int, size_mb float)
    SERVER results OPTIONS (filename '/results/storage.csv', format 'csv', header 'true');
CREATE FOREIGN TABLE q1 (pickup_date date, trips bigint, revenue float, avg_distance float)
    SERVER results OPTIONS (filename '/results/q1.csv', format 'csv', header 'true');

CREATE VIEW bench_v AS SELECT
  CASE query WHEN 'q1' THEN 'Q1 (31 день)' WHEN 'q2' THEN 'Q2 (7 дней)' ELSE 'Q3 (91 день)' END AS "Запрос",
  CASE storage || '/' || variant WHEN 'csv/naive' THEN 'CSV' WHEN 'csv/pruned' THEN 'CSV + дата'
       WHEN 'parquet/naive' THEN 'Parquet' ELSE 'Parquet + pruning' END AS "Вариант",
  median_s AS "Время, с", round(input_mb)::int AS "Прочитано, МБ"
FROM bench;
