from datetime import datetime
from sqlalchemy.orm import Session
from app.models.dataset import Dataset, Dataflow
from app.models.monitor import MonitorCheck

def test_list_datasets(client, db_session: Session):
    d1 = Dataset(id="ds_1", name="Test DS 1", provider_type="mysql", card_count=100)
    d2 = Dataset(id="ds_2", name="Test DS 2", provider_type="csv", card_count=0)
    db_session.add(d1)
    db_session.add(d2)
    db_session.commit()

    response = client.get("/api/monitor/datasets?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 2
    assert len(data["datasets"]) == 2

    response = client.get("/api/monitor/datasets?provider_type=mysql")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["datasets"][0]["id"] == "ds_1"


def test_list_alerts(client, db_session: Session):
    d1 = Dataset(id="failed_ds_1", name="Failed DS", provider_type="mysql", last_execution_state="FAILED", card_count=50)
    df1 = Dataflow(id="failed_df_1", name="Failed DF", status="FAILED", last_execution_state="SUCCESS")
    db_session.add(d1)
    db_session.add(df1)
    db_session.commit()

    response = client.get("/api/monitor/alerts")
    assert response.status_code == 200
    data = response.json()
    assert data["all_ok"] is False 
    assert len(data["failed_datasets"]) == 1
    assert data["failed_datasets"][0]["id"] == "failed_ds_1"
    assert len(data["failed_dataflows"]) == 1
    assert data["failed_dataflows"][0]["id"] == "failed_df_1"
