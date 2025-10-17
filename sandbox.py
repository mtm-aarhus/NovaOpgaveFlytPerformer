from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import os
from robot_framework.process import process
from OpenOrchestrator.database.queues import QueueElement, QueueStatus
import json
from typing import Optional

def make_queue_element_with_payload(
    payload: dict | list,
    queue_name: str,
    reference: Optional[str] = None,
    created_by: Optional[str] = None,
    status: QueueStatus = QueueStatus.NEW, 
) -> QueueElement:
    # Validate & serialize
    data_str = json.dumps(payload, ensure_ascii=False)
    if len(data_str) > 2000:
        raise ValueError("data exceeds 2000 chars (column limit)")

    return QueueElement(
        queue_name=queue_name,
        status=status,
        data=data_str,
        reference=reference,
        created_by=created_by,
    )

qe = make_queue_element_with_payload(
    payload={
    "OprindeligAktivitetsbehandler": "AZMTM01",
    "SagensSagsbehandler": "AZMTM01",
    "NyAktivitetsbehandler": "AZ60026"
},
    queue_name="NovaOpgaveFlyt",
    reference="Sandbox",
    status=QueueStatus.NEW, 
)

orchestrator_connection = OrchestratorConnection(
        "NovaOpgaveFlytPerformer",
        os.getenv("OpenOrchestratorSQL"),
        os.getenv("OpenOrchestratorKey"),
        None
    )

cache = {"cases": {}, "tasks": {}, "caseworkers": {}}

cache = process(orchestrator_connection, qe, cache)
print(cache)