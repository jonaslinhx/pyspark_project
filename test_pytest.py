import pytest

from lib.utils import get_spark_session, get_project_conf
from lib import dataloader
from lib import schemas
from lib import transformations

from chispa import assert_df_equality

@pytest.fixture(scope="session")
def spark():
    return get_spark_session(
        env="LOCAL",
        conf_files=["conf/spark.conf","conf/pyspark_project.conf"]
    )

@pytest.fixture(scope="session")
def expected_contract_df():
    return spark.read.format("json").schema(schemas.contract_schema).load("test_data/results/contract_df.json")

def test_spark_version(spark):

    assert spark.version == "3.5.0"

def test_get_config():
    conf_local: dict = get_project_conf("LOCAL", "conf/pyspark_project.conf")
    conf_qa: dict = get_project_conf("QA", "conf/pyspark_project.conf")
    assert conf_local['kafka.topic'] == "pyspark_project-kafka_cloud"
    assert conf_qa['kafka.topic'] == "pyspark_project-qa"

def test_read_data(spark):
    account_df = dataloader.read_data(spark, "LOCAL", False, None, "account", schemas.account_schema)
    assert account_df .count() == 8
    party_df = dataloader.read_data(spark, "LOCAL", False, None, "party", schemas.party_schema)
    assert party_df .count() == 8
    address_df = dataloader.read_data(spark, "LOCAL", False, None, "address", schemas.address_schema)
    assert address_df .count() == 8

def test_get_contract(spark, expected_contract_df):
    account_df = dataloader.read_data(spark, "LOCAL", False, None, "account", schemas.account_schema)
    actual_contract_df = transformations.get_contract(account_df)
    assert expected_contract_df.collect() == actual_contract_df.collect()
    # chispa is similar to above expect don't need to collect and have schema
    assert_df_equality(expected_contract_df,actual_contract_df, ignore_schema=True)