from pyspark.sql import SparkSession

S3_DATA_INPUT_PATH="s3://s3-test-emr-athena/source-folder/wikiticker-2015-09-12-sampled.json"
S3_DATA_OUTPUT_PATH_FILTERED="s3://s3-test-emr-athena/data-output/filtered"

def main():
    spark = SparkSession.builder.appName('EmrAthenaDemo').getOrCreate()
    df = spark.read.json(S3_DATA_INPUT_PATH)
    print(f'データセットのレコードの総数 {df.count()}')
    filtered_df = df.filter((df.isRobot == False) & (df.countryName == 'United States'))
    print(f'フィルターされたデータセットのレコードの総数 {filtered_df.count()}')
    filtered_df.printSchema()
    filtered_df.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_FILTERED)
    print('データが正常にアップロードされました')

if __name__ == '__main__':
    main()
