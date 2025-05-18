import yaml


def load_csv_data_to_df(spark, file_path):

    return spark.read.option("inferSchema", "true").csv(file_path, header=True)


def read_yaml(file_path):

    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def write_output(df, file_path, write_format):

    df.repartition(1).write.format(write_format).mode("overwrite").option(
        "header", "true"
    ).save(file_path)