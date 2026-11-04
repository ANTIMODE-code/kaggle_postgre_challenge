-- Checksum canonici delle destinazioni, indipendenti dagli ID surrogati (identity).
-- Le chiavi generate vengono sostituite con chiavi naturali:
--   table3.id_column_1      -> contenuto della riga di table3
--   table1.id_column_1      -> table1.id_column_3 (raw_id)
--   table1.id_column_4      -> contenuto della riga di table3 referenziata
--   table2/5/6.id_column_2  -> raw_id della riga di table1 referenziata
-- Così un'implementazione parallela (ID assegnati in ordine diverso) resta confrontabile.
WITH
t3 AS (
    SELECT id_column_1,
           concat_ws('|', text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4) AS natural_key
    FROM public.table3
),
t1 AS (
    SELECT t.id_column_1, t.id_column_3 AS raw_id,
           concat_ws('|', t.id_column_2, t.text_column_1, t.datetime_column_1, t.text_column_2,
                     t.integer_column_1, t.integer_column_2, t.integer_column_3, t.text_column_3,
                     t.id_column_3, t3.natural_key, t.id_column_5, t.integer_column_4,
                     t.integer_column_5, t.id_column_6, t.integer_column_6, t.integer_column_7) AS row_text
    FROM public.table1 AS t
    LEFT JOIN t3 ON t3.id_column_1 = t.id_column_4
),
rows_by_table AS (
    SELECT 'public.table3' AS table_name, natural_key AS row_text FROM t3
    UNION ALL
    SELECT 'public.table4', concat_ws('|', id_column_1, text_column_1, text_column_2) FROM public.table4
    UNION ALL
    SELECT 'public.table1', row_text FROM t1
    UNION ALL
    SELECT 'public.table5', concat_ws('|', t.numeric_column_1, t.numeric_column_2, t.integer_column_1,
                                      t.integer_column_2, t1.raw_id, t.id_column_3)
    FROM public.table5 AS t JOIN t1 ON t1.id_column_1 = t.id_column_2
    UNION ALL
    SELECT 'public.table6', concat_ws('|', t1.raw_id, t.id_column_3)
    FROM public.table6 AS t JOIN t1 ON t1.id_column_1 = t.id_column_2
    UNION ALL
    SELECT 'public.table2', concat_ws('|', t1.raw_id, t.integer_column_1, t.integer_column_2, t.text_column_1,
                                      t.integer_column_3, t.integer_column_4, t.integer_column_5, t.integer_column_6)
    FROM public.table2 AS t JOIN t1 ON t1.id_column_1 = t.id_column_2
)
SELECT json_object_agg(table_name, json_build_object('rows', row_count, 'md5', checksum) ORDER BY table_name)::text
FROM (
    -- md5 della concatenazione ordinata degli md5 di riga: indipendente
    -- dall'ordine fisico e dagli ID surrogati.
    SELECT table_name,
           count(*) AS row_count,
           md5(string_agg(row_hash, '' ORDER BY row_hash)) AS checksum
    FROM (SELECT table_name, md5(row_text) AS row_hash FROM rows_by_table) AS hashed
    GROUP BY table_name
) AS per_table;
