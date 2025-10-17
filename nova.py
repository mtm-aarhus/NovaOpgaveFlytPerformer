import requests
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
import uuid
from datetime import datetime

def get_access_token(orchestrator_connection: OrchestratorConnection):
    NovaToken = orchestrator_connection.get_credential("KMDAccessToken")
    Secret = orchestrator_connection.get_credential("KMDClientSecret")
    

    NovaTokenAPI = NovaToken.username
    secret = Secret.password
    id = Secret.username
    
    # Authenticate
    auth_payload = {
        "client_secret": secret,
        "grant_type": "client_credentials",
        "client_id": id,
        "scope": "client"
    }
    
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(NovaTokenAPI, data=auth_payload, headers=headers)

    response.raise_for_status()
    access_token = response.json().get("access_token")
    return access_token

def compare_caseworker_from_case(sags_uuid, transaction, access_token, KMDNovaURL):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    url = f"{KMDNovaURL}/Case/GetList?api-version=2.0-Case"

    data = {
        "common": {
            "transactionId": transaction,
            "uuid": sags_uuid,
        },
        "paging": {
            "startRow": 1,
            "numberOfRows": 2
        },
        "caseGetOutput": {
            "caseworker": {
                "kspIdentity": {
                    "novaUserId": True,
                    "racfId": True,
                    "fullName": True
                },
                "fkOrgIdentity": {
                    "fkUuid": True,
                    "type": True,
                    "fullName": True
                },
                "losIdentity": {
                    "novaUnitId": True,
                    "administrativeUnitId": True,
                    "fullName": True,
                    "userKey": True
                },
                "caseworkerCtrlBy": True
            }
        }
    }

    response = requests.put(url, headers=headers, json=data)
    response.raise_for_status()
    response_json = response.json()

    if len(response_json.get("cases", [])) != 1:
        return ""

    caseworker = response_json["cases"][0].get("caseworker", {})
    caseworker_id = (
        caseworker.get("kspIdentity", {}).get("racfId")
        or caseworker.get("losIdentity", {}).get("userKey")
        or ""
    ).strip().lower()

    return caseworker_id


def get_task_list(transaction, caseworker, access_token, KMDNovaURL):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    url = f"{KMDNovaURL}/Task/GetList?api-version=2.0-Case"
    start_row = 1
    page_size = 500
    all_tasks = []

    while True:
        data = {
            "common": {
                "transactionId": transaction
            },
            "paging": {
                "startRow": start_row,
                "numberOfRows": page_size
            },
            "caseworker": {
                "kspIdentity": {
                        "racfId": caseworker
                }
            },
            "toStartDate": datetime.now().isoformat(),
            "statusCode": ["S", "N"]
        }

        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()
        response_json = response.json()

        task_page = response_json.get("taskList", [])
        all_tasks.extend(task_page)

        paging_info = response_json.get("pagingInformation", {})
        if not paging_info.get("hasMoreRows", False):
            break

        start_row += page_size

    return all_tasks


def lookup_caseworker_by_racfId(racfId, transaction, access_token, KMDNovaURL):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    # First: Try searching cases
    case_url = f"{KMDNovaURL}/Case/GetList?api-version=2.0-Case"
    case_data = {
        "common": {
            "transactionId": transaction
        },
        "paging": {
            "startRow": 1,
            "numberOfRows": 500
        },
        "caseWorker": {
            "kspIdentity": {
                "racfId": racfId
            }
        },
        "caseGetOutput": {
            "caseAttributes": {
                "userFriendlyCaseNumber": True
            },
            "caseworker": {
                "kspIdentity": {
                    "novaUserId": True,
                    "racfId": True,
                    "fullName": True
                },
                "fkOrgIdentity": {
                    "fkUuid": True,
                    "type": True,
                    "fullName": True
                },
                "losIdentity": {
                    "novaUnitId": True,
                    "administrativeUnitId": True,
                    "fullName": True,
                    "userKey": True
                },
                "caseworkerCtrlBy": True
            }
        }
    }

    case_response = requests.put(case_url, headers=headers, json=case_data)
    case_response.raise_for_status()
    case_json = case_response.json()

    # Check if any case matches the RACF ID
    for case in case_json.get("cases", []):
        caseworker = case.get("caseworker", {})
        ksp = caseworker.get("kspIdentity", {})
        if ksp.get("racfId", "").lower() == racfId.lower():
            return caseworker

    # Second: Try searching tasks
    task_url = f"{KMDNovaURL}/Task/GetList?api-version=2.0-Case"
    task_data = {
        "common": {
            "transactionId": str(uuid.uuid4())
        },
        "caseworker": {
            "kspIdentity": {
                "racfId": racfId
            }
        },
        "paging": {
            "startRow": 1,
            "numberOfRows": 500
        }
    }

    task_response = requests.put(task_url, headers=headers, json=task_data)
    task_response.raise_for_status()
    task_json = task_response.json()

    for task in task_json.get("taskList", []):
        caseworker = task.get("caseworker", {})
        ksp = caseworker.get("kspIdentity", {})
        if ksp.get("racfId", "").lower() == racfId.lower():
            return caseworker

    raise Exception("Aktivitetsovertager kspIdentity not found")


def update_caseworker_task(task, access_token, KMDNovaURL, new_ksp_identity):
    """
    Updates a single task's caseworker via the KMD Nova Task/Update API.
    - Renames task-prefixed fields to match schema.
    - Filters and includes only schema-allowed fields.
    - Replaces the caseworker field with the new kspIdentity.
    """

    # Mapping of Task/GetList fields → Task/Update schema fields
    field_mapping = {
        "taskUuid": "uuid",
        "caseUuid": "caseUuid",
        "taskTitle": "title",
        "taskDescription": "description",
        "taskDeadline": "deadline",
        "taskStartDate": "startDate",
        "taskCloseDate": "closeDate",
        "kle": "kle",
        "taskStatusCode": "statusCode",
        "taskType": "taskType",
        "taskRepeat": "taskRepeat",
    }

    # Extract and rename allowed fields
    transformed_task = {}
    for old_key, new_key in field_mapping.items():
        if old_key == "taskType":
            task_type_obj = task.get(old_key)
            if isinstance(task_type_obj, dict):
                transformed_task[new_key] = task_type_obj.get("taskTypeName")
        else:
            value = task.get(old_key)
            if value is not None:
                transformed_task[new_key] = value

    # Replace caseworker with only kspIdentity
    transformed_task["caseworker"] = {"kspIdentity": new_ksp_identity}

    # Build final payload with flattened structure
    payload = {
        "common": {
            "transactionId": str(uuid.uuid4())
        },
        **transformed_task
    }

    # Perform the update call
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    url = f"{KMDNovaURL}/Task/Update?api-version=2.0-Case"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    return response.status_code
