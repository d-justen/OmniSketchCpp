mkdir -p ../ssb-skew-sketches

cd ../build
./bin/build_sketches --in="$SSB_SKEW_PATH"/lineorder.csv --table_name=lineorder1 --column_names=id,lo_discount,lo_quantity,lo_orderdate,lo_partkey,lo_custkey,lo_suppkey --data_types=uint,uint,uint,uint,uint,uint,uint --out=../ssb-skew-sketches --width=256 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/date.csv --table_name=date1 --column_names=d_datekey,d_year,d_yearmonthnum,d_weeknuminyear,d_yearmonth --data_types=uint,uint,uint,uint,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/part.csv --table_name=part1 --column_names=p_partkey,p_category,p_brand,p_mfgr --data_types=uint,varchar,varchar,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/supplier.csv --table_name=supplier1 --column_names=s_suppkey,s_region,s_nation,s_city --data_types=uint,varchar,varchar,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/customer.csv --table_name=customer1 --column_names=c_custkey,c_region,c_nation,c_city --data_types=uint,varchar,varchar,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/date.csv --ref_sketch=../ssb-skew-sketches/lineorder1__lo_orderdate.json --table_name=date1 --column_names=d_datekey,d_year,d_yearmonthnum,d_weeknuminyear,d_yearmonth --data_types=uint,uint,uint,uint,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/part.csv --ref_sketch=../ssb-skew-sketches/lineorder1__lo_partkey.json --table_name=part1 --column_names=p_partkey,p_category,p_brand,p_mfgr --data_types=uint,varchar,varchar,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/supplier.csv --ref_sketch=../ssb-skew-sketches/lineorder1__lo_suppkey.json --table_name=supplier1 --column_names=s_suppkey,s_region,s_nation,s_city --data_types=uint,varchar,varchar,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
./bin/build_sketches --in="$SSB_SKEW_PATH"/customer.csv --ref_sketch=../ssb-skew-sketches/lineorder1__lo_custkey.json --table_name=customer1 --column_names=c_custkey,c_region,c_nation,c_city --data_types=uint,varchar,varchar,varchar --out=../ssb-skew-sketches --width=32 --cell_size=128
