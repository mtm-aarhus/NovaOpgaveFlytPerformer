from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import json
from nova import get_access_token, get_task_list, lookup_caseworker_by_racfId, compare_caseworker_from_case, update_caseworker_task
import uuid
import os
import json
import sqlite3
from datetime import datetime

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement, cache):
    orchestrator_connection.log_info("Starting process")
    queue_data = json.loads(queue_element.data)

    AktivitetsSagsbehandler = queue_data.get('OprindeligAktivitetsbehandler')
    SagensSagsbehandler = queue_data.get('SagensSagsbehandler')
    AktivitetsOvertager = queue_data.get('NyAktivitetsbehandler')

    access_token = get_access_token(orchestrator_connection)
    Nova_URL = orchestrator_connection.get_constant("KMDNovaURL").value

    if AktivitetsOvertager not in cache["caseworkers"]:
        overtager_caseworker_json = lookup_caseworker_by_racfId(AktivitetsOvertager, str(uuid.uuid4()), access_token, Nova_URL)
        cache["caseworkers"][AktivitetsOvertager] = overtager_caseworker_json
    else:
        overtager_caseworker_json = cache["caseworkers"][AktivitetsOvertager]

    new_ksp_identity = overtager_caseworker_json.get("kspIdentity")
    
    if not new_ksp_identity:
        raise ValueError("Aktivitetsovertager caseworker JSON must contain 'kspIdentity'")

    if AktivitetsSagsbehandler not in cache["tasks"]:
        cache["tasks"][AktivitetsSagsbehandler] = get_task_list(str(uuid.uuid4()), AktivitetsSagsbehandler, access_token, Nova_URL)

    task_list = cache["tasks"][AktivitetsSagsbehandler]

    tasks_to_remove = []
    conn = get_db_connection()

    # ---- Process each task ----
    for task in task_list:
        case_uuid = task.get("caseUuid")

        # Use cached caseworker if available
        if case_uuid not in cache["cases"]:
            caseworker_id = compare_caseworker_from_case(case_uuid, str(uuid.uuid4()), access_token, Nova_URL)
            cache["cases"][case_uuid] = caseworker_id
        else:
            caseworker_id = cache["cases"][case_uuid]

        if caseworker_id.strip().lower() == SagensSagsbehandler.strip().lower():
            orchestrator_connection.log_info(f'Updating task {task.get("taskUuid")} - {task.get("taskTitle")} from {AktivitetsSagsbehandler} to {AktivitetsOvertager} on case {case_uuid}')
            update_caseworker_task(task, access_token, Nova_URL, new_ksp_identity)
            # --- Log to SQLite ---
            conn.execute("""
                INSERT INTO task_log (
                    timestamp, case_uuid, task_uuid, task_title,
                    original_task_json,
                    oprindelig_aktivitetsbehandler, sagens_sagsbehandler, ny_aktivitetsbehandler
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                datetime.now(),  
                case_uuid,
                task.get("taskUuid"),
                task.get("taskTitle"),
                json.dumps(task, ensure_ascii=False),
                AktivitetsSagsbehandler,
                SagensSagsbehandler,
                AktivitetsOvertager

            ))
            conn.commit()

            tasks_to_remove.append(task)

    for task in tasks_to_remove:
        cache["tasks"][AktivitetsSagsbehandler].remove(task)

    return cache

def get_db_connection():
    """Ensure folder + DB exist, return sqlite3 connection."""
    user_folder = os.path.expanduser("~")
    log_folder = os.path.join(user_folder, "OpgaveFlyt")
    os.makedirs(log_folder, exist_ok=True)
    sqlite3.register_adapter(datetime, lambda dt: dt.isoformat(sep=" ", timespec="seconds"))
    sqlite3.register_converter("DATETIME", lambda s: datetime.fromisoformat(s.decode("utf-8")))

    db_path = os.path.join(log_folder, "opgaveflyt_log.db")
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS task_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            case_uuid TEXT,
            task_uuid TEXT,
            task_title TEXT,
            original_task_json TEXT,
            oprindelig_aktivitetsbehandler TEXT,
            sagens_sagsbehandler TEXT,
            ny_aktivitetsbehandler TEXT
        )
    """)
    conn.commit()
    return conn
