from pyspark.sql import SparkSession

S3_DATA_INPUT_PATH="s3://s3-test-emr-athena/source-folder/wikiticker-2015-09-12-sampled.json"
S3_DATA_OUTPUT_PATH_AGGREGATED="s3://s3-test-emr-athena/data-output/aggregated"

def main():
    spark = SparkSession.builder.appName('EmrAthenaDemo1').getOrCreate()
    df = spark.read.json(S3_DATA_INPUT_PATH)
    print(f'データセットのレコードの総数 {df.count()}')
    aggregated_df = df.groupBy(df.channel).count()
    print(f'集計されたデータセットのレコードの総数 {aggregated_df.count()}')
    aggregated_df.show(10)
    aggregated_df.printSchema()
    aggregated_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_AGGREGATED)
    print('データが正常にアップロードされました')


if __name__ == '__main__':
    main()