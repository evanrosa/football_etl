import pytest
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os
from flink.job.football_stream_job import process_competitions

@pytest.fixture
def mock_env_vars(mocker):
    """Fixture to mock environment variables."""
    mocker.patch.dict(os.environ, {
        "KAFKA_BROKER": "localhost:9092",
        "CONFLUENT_STAGE_KEY": "test_key",
        "CONFLUENT_STAGE_SECRET": "test_secret",
        "SPORTS_RADAR_KEY": "test_sports_radar_key"
    })

@pytest.fixture
def mock_stream_environment(mocker):
    """Fixture to mock the StreamExecutionEnvironment."""
    mock_env = mocker.MagicMock(StreamExecutionEnvironment)
    mock_t_env = mocker.MagicMock(StreamTableEnvironment)
    mock_settings = mocker.MagicMock(EnvironmentSettings)

    # Correctly mock the EnvironmentSettings chain
    mock_settings.in_streaming_mode.return_value = mock_settings
    mock_settings.build = mocker.MagicMock(return_value=mock_settings)
    mocker.patch.object(EnvironmentSettings, "new_instance", return_value=mock_settings)

    # Mock StreamExecutionEnvironment
    mocker.patch.object(StreamExecutionEnvironment, "get_execution_environment", return_value=mock_env)

    # Mock StreamTableEnvironment creation
    mocker.patch.object(StreamTableEnvironment, "create", return_value=mock_t_env)

    # Mock SQL query and execution behavior
    mock_t_env.sql_query.return_value.execute_insert.return_value = None

    return mock_env, mock_t_env


def test_process_competitions_env_vars_missing(mocker):
    """Test behavior when environment variables are missing."""
    mocker.patch.dict(os.environ, {}, clear=True)
    with pytest.raises(KeyError):
        process_competitions()

def test_process_competitions_env_vars_loaded(mock_env_vars, mock_stream_environment):
    """Test that environment variables are correctly loaded."""
    env, t_env = mock_stream_environment

    # Execute the function
    process_competitions()

    # Assertions for Kafka broker and credentials
    kafka_source_call = t_env.execute_sql.call_args_list[0][0][0]
    kafka_sink_call = t_env.execute_sql.call_args_list[1][0][0]

    assert "localhost:9092" in kafka_source_call
    assert "test_key" in kafka_source_call
    assert "test_secret" in kafka_source_call

def test_process_competitions_kafka_ddl_execution(mock_env_vars, mock_stream_environment):
    """Test that Kafka source and sink DDLs are executed."""
    env, t_env = mock_stream_environment

    # Execute the function
    process_competitions()

    # Verify DDL execution for Kafka source and sink
    assert t_env.execute_sql.call_count == 2
    assert "CREATE TABLE kafka_source" in t_env.execute_sql.call_args_list[0][0][0]
    assert "CREATE TABLE kafka_sink" in t_env.execute_sql.call_args_list[1][0][0]

def test_process_competitions_query_execution(mock_env_vars, mock_stream_environment):
    """Test that the SQL query to transform data is executed."""
    env, t_env = mock_stream_environment

    # Execute the function
    process_competitions()

    # Verify that the SQL query is executed
    t_env.sql_query.assert_called_once()
    query_call = t_env.sql_query.call_args[0][0]
    assert "SELECT id, region, name FROM kafka_source" in query_call

    # Verify data insertion into the sink
    t_env.sql_query.return_value.execute_insert.assert_called_once_with("kafka_sink")

def test_process_competitions_checkpoint_config(mock_env_vars, mock_stream_environment):
    """Test checkpoint configurations for the environment."""
    env, _ = mock_stream_environment

    # Execute the function
    process_competitions()

    # Assertions for checkpoint configurations
    env.get_checkpoint_config.assert_called_once()
    checkpoint_config = env.get_checkpoint_config.return_value

    checkpoint_config.set_min_pause_between_checkpoints.assert_called_once_with(5000)
    checkpoint_config.set_checkpoint_timeout.assert_called_once_with(60000)
    checkpoint_config.set_max_concurrent_checkpoints.assert_called_once_with(1)
