from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import os
import json
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from pathlib import Path
import pandas as pd
import math

#Henter de nødvendige cookies
def cookie_getter(username, password):
    try:
        # Find the path of the Selenium WebDriver manager
        selenium_path_parts = Path(__file__).resolve().parent.parts
        if "selenium.webdriver" in selenium_path_parts:
            selenium_folder_index = selenium_path_parts.index("selenium.webdriver")
            if selenium_folder_index < len(selenium_path_parts) - 1:
                version_folder_path = Path(*selenium_path_parts[:selenium_folder_index + 2])
                executable_path = next(version_folder_path.rglob("*manager*.exe"), None)
                if executable_path:
                    os.environ["SE_MANAGER_PATH"] = str(executable_path)

        # Get Chrome user data directory
        app_data_path = os.getenv("LOCALAPPDATA")
        chrome_user_data_path = os.path.join(app_data_path, "Google", "Chrome", "User Data")

        # Set up Chrome options
        options = Options()
        options.add_argument("--headless=new")  # Headless mode
        options.add_argument(f"--user-data-dir={chrome_user_data_path}")
        options.add_argument("--window-size=1920,900")
        options.add_argument("--start-maximized")
        options.add_argument("force-device-scale-factor=0.5")
        options.add_argument("--disable-extensions")
        options.add_argument("--profile-directory=Default")
        options.add_argument('--remote-debugging-pipe')

        # Initialize Chrome WebDriver
        service = Service()  # Set correct path to ChromeDriver
        driver = webdriver.Chrome(service=service, options=options)
        driver.get("https://cap-wsswlbs-wm3q2021.kmd.dk/KMD.YH.KMDLogonWEB.NET/AspSson.aspx?"
                "KmdLogon_sApplCallback=https://cap-awswlbs-wm3q2021.kmd.dk/KMDNovaESDH/forside"
                "&KMDLogon_sProtocol=tcpip&KMDLogon_sApplPrefix=--&KMDLogon_sOrigin=ApplAsp&ExtraData=true")

        wait = WebDriverWait(driver, 10)
        wait.until(EC.visibility_of_element_located((By.NAME, "UserInfo.Username"))).send_keys(username)
        wait.until(EC.visibility_of_element_located((By.NAME, "UserInfo.Password"))).send_keys(password)
        wait.until(EC.element_to_be_clickable((By.ID, "logonBtn"))).click()

        try:
            logs = driver.get_log("browser")
            for log in logs:
                print(log["message"])
        except Exception:
            print("No browser logs available.")

        # Retrieve cookies
        cookies_list = driver.get_cookies()
        for cookie in cookies_list:
            print(f"Name: {cookie['name']}, Value: {cookie['value']}, HttpOnly: {cookie['httpOnly']}")

        # Function to retrieve specific cookie values
        def get_cookie_value(cookies, name):
            for cookie in cookies:
                if cookie['name'] == name:
                    return cookie['value']
            return None

        # Retrieve specific cookies
        out_verification_token = get_cookie_value(cookies_list, "__RequestVerificationToken_L0tNRE5vdmFFU0RI0") ##VerifikationToken
        out_kmd_logon_web_session_handler = get_cookie_value(cookies_list, "KMDLogonWebSessionHandler") ##NovaLogonWebSession

        # Find element with XPath
        elements = driver.find_elements(By.XPATH, "/html/body/input[1]")
        out_request_verification_token = None

        if elements:
            element = elements[0]
            out_request_verification_token = element.get_attribute("ncg-request-verification-token") ##RequestVerifikation
        else:
            print("Element not found")

        # Close the browser
        driver.quit()
    except Exception as e:
        print(e)
        raise
    return out_verification_token, out_kmd_logon_web_session_handler, out_request_verification_token

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement, sagsliste) :
    orchestrator_connection.log_info("Starting process")
    queue_element = json.loads(queue_element.data)

    AktivitetsSagsbehandler = queue_element.get('OprindeligAktivitetsbehandler', None)
    SagensSagsbehandler = queue_element.get('SagensSagsbehandler', None)
    AktivitetsOvertager =   queue_element.get('NyAktivitetsbehandler', None)
    NovaLogin = orchestrator_connection.get_credential('KMDNovaRobotLogin')

    # Log in
    username = NovaLogin.username
    password = NovaLogin.password

    #Getting the cookies
    out_verification_token, out_kmd_logon_web_session_handler, out_request_verification_token = cookie_getter(username, password)
        
        
    #Sagsbehandler findes, de nødvendige parametre til overskrivning af gammel sagsbehandler defineres her
    CaseWorker = str(AktivitetsOvertager).upper()

    # Define the URL with the caseworker parameter
    base_url = f"https://cap-awswlbs-wm3q2021.kmd.dk/KMDNovaESDH/api/ServiceRelayer/KMDNova/v1/lookup/caseworker?AdmEnhedsId=325&IncludeOrganizationUnits=true&SearchString={CaseWorker}"
    params = {
        "AdmEnhedsId": 325,
        "IncludeOrganizationUnits": "true",
        "SearchString": CaseWorker  
    }

    # Define headers
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/json;charset=UTF-8",
        "RequestVerificationToken": out_request_verification_token, 
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.55",
        "X-Requested-With": "XMLHttpRequest",
        "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Microsoft Edge\";v=\"109\", \"Chromium\";v=\"109\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    # Define cookies
    cookies = {
        "kmdNovaIndstillingerCurrent": "MTM-Byggeri",
        "__RequestVerificationToken_L0tNRE5vdmFFU0RI0": out_verification_token ,  
        "KMDLogonWebSessionHandler": out_kmd_logon_web_session_handler  
    }

    # Make the GET request
    response = requests.get(base_url, headers=headers, cookies=cookies, params=params)
    response.raise_for_status()

    # Info om aktivitetsovertager hentes 
    try:
        json_response = response.json()

        # Extract CaseworkerUser where UserId matches CaseWorker
        caseworker_users = json_response.get("Caseworkers", [])
        caseworker_user = next(
            (cw["CaseworkerUser"] for cw in caseworker_users if cw["CaseworkerUser"]["UserId"] == CaseWorker),
            None)

        if caseworker_user:
            # Extract required fields
            filtered_caseworker = {
                "DisplayName": caseworker_user["DisplayName"],
                "UserId": caseworker_user["UserId"],
                "Name": caseworker_user["Name"],
                "MunicipalityNumber": caseworker_user.get("MunicipalityNumber")
            }

            # Convert to JSON format
            ResponseOut_caseworker = json.dumps(filtered_caseworker, indent=4)
            orchestrator_connection.log_info(f'Output ny caseworker:{ResponseOut_caseworker}')
        else:
            orchestrator_connection.log_info("No matching caseworker found.")
            raise ValueError("No matching caseworker found.")

    except json.JSONDecodeError:
        orchestrator_connection.log_info("Error decoding JSON response.")
        raise ValueError("Invalid JSON response received.")

    orchestrator_connection.log_info(f'Laver kørsel for følgende sagsbehandlere {AktivitetsSagsbehandler}, {SagensSagsbehandler}, {AktivitetsOvertager}')

    #Nu søges der efter aktiviteter på den gamle sagsbehandler; de forskellige ting puttes i caseworkerout
    AktiviteterSendt = 0

    # Define URL
    url = "https://cap-awswlbs-wm3q2021.kmd.dk/KMDNovaESDH/api/ServiceRelayer/kmdnova/v1/task/AdvancedSearch"

    # Define Request Body
    body = {
        "Criteria": {
            "ResponsibleOrgUnitId": "",
            "CaseworkerId": AktivitetsSagsbehandler, 
            "CaseworkerGroupId": "",
            "CloseDatePeriod": None,
            "CreateDatePeriod": None,
            "StartDatePeriod": {
                "StandardDatePeriod": "DateInterval",
                "FromDate": None,
                "ToDate": None,
                "DaysAfter": 0
            },
            "DeadlinePeriod": None,
            "Description": None,
            "Statuses": ["NotStarted", "Started"],
            "Title": None,
            "SortOrder": {
                "Id": "DeadlineDateDesc",
                "DisplayName": "Fristdato (Faldende)"
            },
            "KleClassificationTopics": [],
            "SelectedTaskTypeCodeList": []
        },
        "NumberOfTasksAlreadySent": AktiviteterSendt  
    }

    # Make the POST request
    response = requests.post(url, headers=headers, cookies=cookies, json=body)
    response.raise_for_status()

    # Get the response content
    CaseworkerOut = json.loads(response.text)

    #Det udregnes, hvor mange gange koden skal køre
    TotalAntalAktiviteter = CaseworkerOut.get('ItemsCount', None)
    AntalKørsler = math.ceil(TotalAntalAktiviteter/2000)

    #Vi laver en tabel
    datatable = pd.DataFrame(columns=["UpdateActivity"])  # Define column structure

    orchestrator_connection.log_info(f'I alt {TotalAntalAktiviteter} aktiviteter')

    #Vi erstatter det antal gange vi skal
    while AntalKørsler > 0:
        # Define request body (JSON)
        body = {
            "Criteria": {
                "ResponsibleOrgUnitId": "",
                "CaseworkerId": AktivitetsSagsbehandler,  
                "CaseworkerGroupId": "",
                "CloseDatePeriod": None,
                "CreateDatePeriod": None,
                "StartDatePeriod": {
                    "StandardDatePeriod": "DateInterval",
                    "FromDate": None,
                    "ToDate": None,
                    "DaysAfter": 0
                },
                "DeadlinePeriod": None,
                "Description": None,
                "Statuses": ["NotStarted", "Started"],
                "Title": None,
                "SortOrder": {
                    "Id": "DeadlineDateDesc",
                    "DisplayName": "Fristdato (Faldende)"
                },
                "KleClassificationTopics": [],
                "SelectedTaskTypeCodeList": []
            },
            "NumberOfTasksAlreadySent": AktiviteterSendt  
        }

        # Send POST request
        response = requests.post(url, headers=headers, cookies=cookies, json=body)
        response.raise_for_status()

        # Get response content
        ResponseOut = response.text
        AktiviteterSendt += 2000

        #Vi kører hvis der er aktiviteter
        if '"ItemsCount":0}' not in ResponseOut:
            orchestrator_connection.log_info(f'Kigger på aktiviteter i intervallet {AktiviteterSendt - 2000} - {AktiviteterSendt}')

            response_json = json.loads(ResponseOut)

            item_array = response_json.get('Items', []) 
            

            for element in item_array:
                SagsNummer = str(element['CaseNumber'])
                existing_casenumber = next((item for item in sagsliste if item[0] == SagsNummer), None)
                orchestrator_connection.log_info(f'Processing {element}')
                
                if existing_casenumber:
                    NovaSagensSagsbehandler = existing_casenumber[1]
                else:
                    orchestrator_connection.log_info(f'Nyt sagsnummer processeres {SagsNummer}')
                    
                    # Construct the API URL
                    url = f"https://cap-awswlbs-wm3q2021.kmd.dk/KMDNovaESDH/api/ServiceRelayer/KMDNova/v1/mainSearch/search?v=3.5.2.0&Id={SagsNummer}&SearchObjectType=Case"

                    # Send GET request
                    response = requests.get(url, headers=headers, cookies=cookies)
                    response.raise_for_status()

                    # Get response content
                    ResponseOut = response.text
                    ResponseSagsinformation = json.loads(ResponseOut)
                    orchestrator_connection.log_info('Sagnummer not in list')
                # Extract nested value safely
                    has_caseworker_user = (ResponseSagsinformation.get("Case", {}).get("Caseworker", {}).get("HasCaseworkerUser", False))

                    if has_caseworker_user:
                        NovaSagensSagsbehandler = str(ResponseSagsinformation.get("Case").get("Caseworker").get("CaseworkerUser").get("UserId"))
                        orchestrator_connection.log_info(f'Har sagsbehandler {NovaSagensSagsbehandler}')
                    else:
                        try:
                            NovaSagensSagsbehandler = str(ResponseSagsinformation.get("Case").get("Caseworker").get("CaseworkerUnit").get("ShortName"))
                            orchestrator_connection.log_info(f'Ingen officiel sagsbehandler, vælger unit {NovaSagensSagsbehandler}')
                        except Exception:
                            raise Exception
                    sagsliste.append([SagsNummer, NovaSagensSagsbehandler])
                if NovaSagensSagsbehandler.lower() == SagensSagsbehandler.lower():
                    orchestrator_connection.log_info(f"Match fundet: {NovaSagensSagsbehandler}")
                    element['Caseworker']['CaseworkerUser'] = json.loads(ResponseOut_caseworker)
                    UpdateActivity = element
                    orchestrator_connection.log_info(f'Færdig streng {UpdateActivity}')
                    # Create a new row
                    new_row = {"UpdateActivity": UpdateActivity}  # Assuming UpdateActivity is a variable

                    # Append row to DataFrame
                    datatable = pd.concat([datatable, pd.DataFrame([new_row])], ignore_index=True)
        else:
            orchestrator_connection.log_info('0 aktiviteter')
        AntalKørsler -= 1

    # Get the user home directory
    user_profile = os.environ.get("USERPROFILE", "C:\\Users\\Default")

    # Construct the full path correctly
    csv_file_path = os.path.join(user_profile, "Documents", "CSV_Folder")

    # Step 2: Create Folder (Equivalent to UiPath Create Folder Activity)
    os.makedirs(csv_file_path, exist_ok=True)  # Creates folder if it doesn’t exist

    # Step 4: Construct the CSV File Name (Equivalent to UiPath Dynamic File Naming)
    csv_filename = f"{AktivitetsSagsbehandler}-{SagensSagsbehandler}-{AktivitetsOvertager}.csv"

    # Step 5: Write DataFrame to CSV (Equivalent to UiPath Write CSV)
    csv_full_path = os.path.join(csv_file_path, csv_filename)
    datatable.to_csv(csv_full_path, index=False, encoding="utf-8")

    # Step 6: Iterate Through DataFrame Rows (Equivalent to UiPath For Each Row)
    # for index, row in datatable.iterrows():
    #     UpdateActivity = row['UpdateActivity']
    #     orchestrator_connection.log_info(f'robotten ændrer følgende aktivitet {UpdateActivity}')

    #     # Define API URL
    #     url = "https://cap-awswlbs-wm3q2021.kmd.dk/KMDNovaESDH/api/ServiceRelayer/kmdnova/v1/task/EditTask"

    #     # Define Request Body
    #     body = json.dumps(UpdateActivity)  # Ensure this is a JSON object or string

    #     # Make the POST Request
    #     response = requests.post(url, headers=headers, cookies=cookies, data=body, timeout=None)
    #     response.raise_for_status()  # Raise error for bad responses (4xx, 5xx) 
    return sagsliste