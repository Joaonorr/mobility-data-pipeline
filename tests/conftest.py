import pytest

from mobility_pipeline.session import create_spark_session


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session("mobility-tests")
    yield session
    session.stop()
