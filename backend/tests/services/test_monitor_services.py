import pytest
from datetime import datetime
from unittest.mock import MagicMock
from app.services.dataset_service import DatasetCrawlService
from app.services.dataflow_service import DataflowCrawlService
from app.services.health_check_service import HealthCheckService
from app.services.monitor import MonitorService

@pytest.fixture
def mock_api():
    return MagicMock()

@pytest.fixture
def mock_db():
    return MagicMock()

def test_dataset_crawl_service_crawl_all_datasets(mock_api, mock_db):
    service = DatasetCrawlService(mock_api, mock_db)
    
    # Mocking API response for post
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "dataSources": [
            {"id": "1", "name": "Test DS 1", "rowCount": 100},
            {"id": "2", "name": "Test DS 2", "rowCount": 200}
        ],
        "_metaData": {"totalCount": 2}
    }
    mock_api.post.return_value = mock_response

    datasets = service.crawl_all_datasets()
    
    assert len(datasets) == 2
    assert datasets[0]["id"] == "1"
    assert datasets[0]["name"] == "Test DS 1"
    assert mock_api.post.called


def test_dataflow_crawl_service_process_dataflow(mock_api, mock_db):
    service = DataflowCrawlService(mock_api, mock_db)
    
    df_stub = {"databaseId": "df_1", "name": "Test DF", "status": "PENDING"}
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"beginTime": 1600000000000, "state": "SUCCESS"}
    ]
    mock_api.get.return_value = mock_response
    
    result = service.process_dataflow(df_stub)
    
    assert result["id"] == "df_1"
    assert result["name"] == "Test DF"
    assert result["last_execution_state"] == "SUCCESS"
    assert result["status"] == "PENDING"
    assert mock_api.get.called


def test_health_check_service_no_alerts(mock_api, mock_db):
    ds_service = MagicMock()
    df_service = MagicMock()
    
    # We need to explicitly patch datetime inside health_check_service or just use actual times
    from datetime import datetime, timezone
    
    # Fix the mock return values to just pass the datetime threshold
    now = datetime.now(timezone.utc)
    recent_timestamp_ms = now.timestamp() * 1000
    
    ds_service.crawl_all_datasets.return_value = [{"id": "ds_1", "card_count": 50, "provider_type": "mysql"}]
    ds_service.fetch_dataset_detail.return_value = {"id": "ds_1", "last_updated": recent_timestamp_ms}
    
    df_service.crawl_all_dataflows.return_value = [{"id": "df_1", "status": "SUCCESS"}]
    df_service.process_dataflow.return_value = {
        "id": "df_1", 
        "status": "SUCCESS", 
        "last_execution_state": "SUCCESS", 
        "last_execution_time": now
    }

    health_service = HealthCheckService(mock_api, mock_db, ds_service, df_service)
    
    result = health_service.check_health(stale_hours=24, min_card_count=0, provider_type="")
    
    assert len(result["alerts"]) == 0
    assert result["summary"]["datasets"]["checked"] == 1
    assert result["summary"]["datasets"]["ok"] == 1
    assert result["summary"]["dataflows"]["checked"] == 1
    assert result["summary"]["dataflows"]["ok"] == 1


def test_monitor_facade_delegation(mock_api, mock_db):
    facade = MonitorService(mock_api, mock_db)
    
    # It should have initialized the three services
    assert facade._dataset_service is not None
    assert facade._dataflow_service is not None
    assert facade._health_check_service is not None
    
    # Test one delegation
    facade._dataset_service.crawl_all_datasets = MagicMock(return_value=["mocked"])
    
    result = facade.crawl_all_datasets()
    assert result == ["mocked"]
    facade._dataset_service.crawl_all_datasets.assert_called_once()
