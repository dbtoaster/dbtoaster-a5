#! /bin/zsh


alpha4="/home/sykora/src/DBToaster/cornell_db_maybms/dbtoaster/compiler/alpha4"
setup="$alpha4/experiments/triggers/repetitive/postgres/tpch/schema_definition.postgres.sql"
for query in 17 18 22 3; do
    agenda="$alpha4/test/data/tpch_w_del/unified_agenda/query${query}.csv"
    query_string="$alpha4/experiments/triggers/repetitive/postgres/tpch/query${query}.sql"
    output="$alpha4/test/scripts/query${query}.log.txt"

    python test_runner.py $query_string $agenda $setup $output 5
done
