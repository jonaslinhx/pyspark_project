import pytest

from lib.utils import get_spark_session

@pytest.fixture(scope="session")
def spark():
    return get_spark_session(
        env="LOCAL",
        conf_files=["conf/spark.conf","conf/pyspark_project.conf"]
    )

def test_blank_test(spark):

    assert spark.version == "3.5.0"