mkdir -p ../job-light-sketches

cd ../build

./bin/build_sketches --in="$JOB_LIGHT_PATH"/imdb/cast_info.csv --table_name=cast_info --column_names=id,movie_id,role_id --data_types=uint,uint,uint --width=256 --cell_size=512 --out=../job-light-sketches
./bin/build_sketches --in="$JOB_LIGHT_PATH"/imdb/movie_companies.csv --table_name=movie_companies --column_names=id,movie_id,company_id,company_type_id --data_types=uint,uint,uint,uint --width=256 --cell_size=512 --out=../job-light-sketches
./bin/build_sketches --in="$JOB_LIGHT_PATH"/imdb/movie_info_idx.csv --table_name=movie_info_idx --column_names=id,movie_id,info_type_id --data_types=uint,uint,uint --width=256 --cell_size=512 --out=../job-light-sketches
./bin/build_sketches --in="$JOB_LIGHT_PATH"/imdb/movie_info.csv --table_name=movie_info --column_names=id,movie_id,info_type_id --data_types=uint,uint,uint --width=256 --cell_size=512 --out=../job-light-sketches
./bin/build_sketches --in="$JOB_LIGHT_PATH"/imdb/movie_keyword.csv --table_name=movie_keyword --column_names=id,movie_id,keyword_id --data_types=uint,uint,uint --width=256 --cell_size=512 --out=../job-light-sketches
./bin/build_sketches --in="$JOB_LIGHT_PATH"/imdb/title.csv --table_name=title --column_names=id,kind_id,production_year --data_types=uint,uint,uint --width=256 --cell_size=512 --out=../job-light-sketches
