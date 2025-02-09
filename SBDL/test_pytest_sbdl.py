import pytest
from lib.Utils import get_spark_session
'''
1. What is a pytest fixture?
A fixture is a function that provides a reusable resource (e.g., a database connection, configuration, or in your case, a Spark session) to test functions. Fixtures eliminate repetitive setup/teardown code across tests.

2. The scope='session' Parameter
Purpose: Specifies how often the fixture is created/reused.
Session scope: The fixture is created once at the start of the test session and reused for all tests in the session. This is ideal for expensive resources (like initializing a Spark session) that can be safely shared across tests.

Scope	Description
session	Created once per test run (shared across all tests).
module	Created once per module (shared across tests in the same file).
class	Created once per test class.
function	Default: Created fresh for every test function.
'''
@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")

def test_blank_tes(spark):
    print(spark.version)
    assert spark.version == "3.4.3"