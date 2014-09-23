#!/bin/bash

DBT_BIN=bin/dbtoaster
DBT_BIN_FLAGS="-d mt"
#DBT_BIN_FLAGS=""
TPCH_REWRITER=../../experiments/triggers/repetitive/scripts/tpch_csv_rewriter.py
MDDB_REWRITER=../../experiments/triggers/repetitive/scripts/mddb_csv_rewriter.py

DBT_DATA=../../experiments/data

# Dataset sizes: 0 - small, 1 - normal, 2 - large, 3 - custom
DATASET_SIZE=3

if [ "$DATASET_SIZE" -eq "0" ]; then 

FINANCIAL_DATASETS="finance/tiny \
                    finance/standard \
                    finance/big"
TPCH_DATASETS="tpch/tiny \
               tpch/standard"
TPCH_DATASETS_DEL="tpch/tiny_del \
                   tpch/standard_del"
MDDB_DATASETS="mddb/standard"


elif [ "$DATASET_SIZE" -eq "1" ]; then

FINANCIAL_DATASETS="finance/tiny \
                    finance/standard \
                    finance/big"
TPCH_DATASETS="tpch/tiny \
               tpch/standard \
               tpch/big \
               tpch/100"
TPCH_DATASETS_DEL="tpch/tiny_del \
                   tpch/standard_del \
                   tpch/big_del \
                   tpch/100_del"
MDDB_DATASETS="mddb/standard"
  
elif [ "$DATASET_SIZE" -eq "2" ]; then

FINANCIAL_DATASETS="finance/tiny \
                    finance/standard \
                    finance/big \
                    finance/huge"
TPCH_DATASETS="tpch/100 \
               tpch/500 \
               tpch/1G  \
               tpch/5G  \
               tpch/10G \
               tpch/tiny \
               tpch/standard \
               tpch/big \
               tpch/huge"
TPCH_DATASETS_DEL="tpch/100_del \
                   tpch/500_del \
                   tpch/1G_del  \
                   tpch/5G_del  \
                   tpch/10G_del \
                   tpch/tiny_del \
                   tpch/standard_del \
                   tpch/big_del \
                   tpch/huge_del"
MDDB_DATASETS="mddb/standard" 

elif [ "$DATASET_SIZE" -eq "3" ]; then

FINANCIAL_DATASETS=""
TPCH_DATASETS=""
TPCH_DATASETS_DEL="tpch/big_del"
MDDB_DATASETS=""

fi

FINANCIAL_QUERIES="finance/axfinder.sql \
                   finance/brokerspread.sql \
                   finance/brokervariance.sql \
                   finance/chrissedtrades.sql \
                   finance/missedtrades.sql \
                   finance/pricespread.sql \
                   finance/vwap.sql"

TPCH_QUERIES="tpch/query1.sql \
              tpch/query10.sql \
              tpch/query11.sql \
              tpch/query11a.sql \
              tpch/query11b.sql \
              tpch/query11c.sql \
              tpch/query12.sql \
              tpch/query13.sql \
              tpch/query14.sql \
              tpch/query15.sql \
              tpch/query16.sql \
              tpch/query17.sql \
              tpch/query17a.sql \
              tpch/query18.sql \
              tpch/query18a.sql \
              tpch/query19.sql \
              tpch/query2.sql \
              tpch/query20.sql \
              tpch/query21.sql \
              tpch/query22.sql \
              tpch/query22a.sql \
              tpch/query3.sql \
              tpch/query4.sql \
              tpch/query5.sql \
              tpch/query6.sql \
              tpch/query7.sql \
              tpch/query8.sql \
              tpch/query9.sql \
              tpch/ssb4.sql"

MDDB_QUERIES="mddb/query1.sql \
              mddb/query2.sql \
              mddb/query2_inner.sql \
              mddb/query3.sql"

TPCH_QUERIES="tpch/query3.sql"

AGENDAS_INPUT_DIR=test/agendas
AGENDAS_OUTPUT_DIR=../../experiments/data/agendas

  
generate_financial_agendas() {
  local INPUT_DIR=$1
  local OUTPUT_DIR=$2
  local DATASETS=$3
  local QUERIES=$4

  local TMP_INPUT_FILE=`mktemp -q /tmp/dbtoaster.XXXXXX`
  local TMP_CPP_EXE_FILE=`mktemp -q /tmp/dbtoaster.XXXXXX`

  for DATASET in $DATASETS
  do
    local DATASET_PATH=$DBT_DATA/$DATASET

    if [ -d "$DATASET_PATH" ]; then

      mkdir -p $OUTPUT_DIR/$DATASET 

      for QUERY in $QUERIES
      do
        echo "Generating the agenda file for $QUERY and $DATASET dataset..."

        local INPUT_FILE=$INPUT_DIR/$QUERY
        local FILENAME=$(basename $INPUT_FILE)
        local OUTPUT_FILENAME="${FILENAME%.*}"_agenda.csv

        sed -e "s:@@DATASET@@:$DATASET_PATH:g" $INPUT_FILE > $TMP_INPUT_FILE
        $DBT_BIN $TMP_INPUT_FILE -l cpp -c $TMP_CPP_EXE_FILE $DBT_BIN_FLAGS
        $TMP_CPP_EXE_FILE -u global
        mv Events.dbtdat $OUTPUT_DIR/$DATASET/$OUTPUT_FILENAME
      done
    fi
  done

  rm -fr $TMP_INPUT_FILE $TMP_CPP_EXE_FILE
}

generate_tpch_agendas() {
  local INPUT_DIR=$1
  local OUTPUT_DIR=$2
  local DATASETS=$3
  local QUERIES=$4
  local DELETIONS=$5 

  local TMP_INPUT_FILE=`mktemp -q /tmp/dbtoaster.XXXXXX`
  local TMP_CPP_EXE_FILE=`mktemp -q /tmp/dbtoaster.XXXXXX`

  for DATASET in $DATASETS
  do
    local DATASET_PATH=$DBT_DATA/$DATASET

    if [ -d "$DATASET_PATH" ]; then

      mkdir -p $OUTPUT_DIR/$DATASET

      for QUERY in $QUERIES
      do
        echo "Generating the agenda file for $QUERY and $DATASET dataset..."

        local INPUT_FILE=$INPUT_DIR/$QUERY
        local FILENAME=$(basename $INPUT_FILE)
        local OUTPUT_FILENAME="${FILENAME%.*}"_agenda.csv

        sed -e "s:@@DATASET@@:$DATASET_PATH:g" $INPUT_FILE | sed "s:@@DELETIONS@@:$DELETIONS:g" > $TMP_INPUT_FILE
        $DBT_BIN $TMP_INPUT_FILE -l cpp -c $TMP_CPP_EXE_FILE $DBT_BIN_FLAGS
        $TMP_CPP_EXE_FILE -u global
        python $TPCH_REWRITER Events.dbtdat $OUTPUT_DIR/$DATASET/$OUTPUT_FILENAME 
        rm -f Events.dbtdat
      done
    fi
  done

  rm -fr $TMP_INPUT_FILE $TMP_CPP_EXE_FILE
}

generate_mddb_agendas() {
  local INPUT_DIR=$1
  local OUTPUT_DIR=$2
  local DATASETS=$3
  local QUERIES=$4
  local DELETIONS=$5

  local TMP_INPUT_FILE=`mktemp -q /tmp/dbtoaster.XXXXXX`
  local TMP_CPP_EXE_FILE=`mktemp -q /tmp/dbtoaster.XXXXXX`

  for DATASET in $DATASETS
  do
    local DATASET_PATH=$DBT_DATA/$DATASET

    if [ -d "$DATASET_PATH" ]; then

      mkdir -p $OUTPUT_DIR/$DATASET

      for QUERY in $QUERIES
      do
        echo "Generating the agenda file for $QUERY and $DATASET dataset..."

        local INPUT_FILE=$INPUT_DIR/$QUERY
        local FILENAME=$(basename $INPUT_FILE)
        local OUTPUT_FILENAME="${FILENAME%.*}"_agenda.csv

        sed -e "s:@@DATASET@@:$DATASET_PATH:g" $INPUT_FILE > $TMP_INPUT_FILE
        $DBT_BIN $TMP_INPUT_FILE -l cpp -c $TMP_CPP_EXE_FILE $DBT_BIN_FLAGS
        $TMP_CPP_EXE_FILE -u global
        python $MDDB_REWRITER Events.dbtdat $OUTPUT_DIR/$DATASET/$OUTPUT_FILENAME
        rm -f Events.dbtdat
      done
    fi
  done

  rm -fr $TMP_INPUT_FILE $TMP_CPP_EXE_FILE
}

generate_financial_agendas $AGENDAS_INPUT_DIR $AGENDAS_OUTPUT_DIR "$FINANCIAL_DATASETS" "$FINANCIAL_QUERIES"
generate_tpch_agendas $AGENDAS_INPUT_DIR $AGENDAS_OUTPUT_DIR "$TPCH_DATASETS" "$TPCH_QUERIES" false
generate_tpch_agendas $AGENDAS_INPUT_DIR $AGENDAS_OUTPUT_DIR "$TPCH_DATASETS_DEL" "$TPCH_QUERIES" true
generate_mddb_agendas $AGENDAS_INPUT_DIR $AGENDAS_OUTPUT_DIR "$MDDB_DATASETS" "$MDDB_QUERIES"

