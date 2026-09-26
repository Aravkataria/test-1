"""
Sample vulnerable utility program to test VAJRA Security Auditor.
Contains 5 classic vulnerabilities (CWE-489, CWE-94, CWE-78, CWE-502, CWE-295).
"""

import os
import pickle
import requests
import flask

app = flask.Flask(__name__)


# 1. [CRITICAL] Code Injection via eval() (CWE-94)
def calculate_expression(user_input: str):
    # Vulnerability: Calling eval() directly on untrusted user input
    result = eval(user_input)
    return result


# 2. [CRITICAL] OS Command Injection via os.system() (CWE-78)
def ping_server(target_ip: str):
    # Vulnerability: Passing unsanitized input to the system shell
    os.system("ping -c 1 " + target_ip)


# 3. [CRITICAL] Insecure Deserialization via pickle (CWE-502)
def load_user_session(session_data: bytes):
    # Vulnerability: Loading arbitrary serialized objects can trigger RCE
    user_obj = pickle.loads(session_data)
    return user_obj


# 4. [HIGH] TLS Certificate Verification Disabled (CWE-295)
def fetch_external_status(url: str):
    # Vulnerability: Disabling SSL/TLS checks leaves traffic open to MITM attacks
    response = requests.get(url, verify=False)
    return response.status_code


# 5. [HIGH] Active Debug Flag in Production (CWE-489)
if __name__ == "__main__":
    # Vulnerability: debug=True exposes interactive debuggers and stack traces
    app.run(host="0.0.0.0", port=5000, debug=True)
