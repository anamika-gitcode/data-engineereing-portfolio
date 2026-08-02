"""
Denodo: Automated VDP Health Check & Backup Trigger
--------------------------------------------------------------
Monitors Denodo server health via its REST API and triggers a
timestamped VDP backup. Designed to run on a schedule (e.g. via
Airflow or cron) with alerting on failure.

Author: Ana
"""

import logging
from datetime import datetime
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("denodo_monitor")


def trigger_alert(message: str):
    """Send alert notification (e.g. Slack, email, PagerDuty)."""
    logger.error(f"ALERT: {message}")


def check_denodo_service_health(base_url: str, auth: tuple) -> bool:
    """Check whether the Denodo server is up and responding."""
    try:
        response = requests.get(f"{base_url}/server/status", auth=auth, timeout=10)
        if response.status_code == 200 and response.json().get("status") == "UP":
            logger.info("Denodo service healthy")
            return True
        raise Exception(f"Unexpected status: {response.status_code}")
    except Exception as e:
        trigger_alert(f"Denodo health check failed: {e}")
        return False


def trigger_vdp_backup(base_url: str, auth: tuple, vdp_name: str):
    """Trigger a timestamped backup of the specified VDP."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_endpoint = f"{base_url}/server/vdp/{vdp_name}/backup"
    try:
        response = requests.post(backup_endpoint, auth=auth, params={"tag": timestamp})
        response.raise_for_status()
        logger.info(f"Backup triggered for {vdp_name} at {timestamp}")
    except requests.exceptions.RequestException as e:
        trigger_alert(f"Backup failed for {vdp_name}: {e}")


if __name__ == "__main__":
    BASE_URL = "https://denodo-server.example.com"
    AUTH = ("admin", "REDACTED")

    if check_denodo_service_health(BASE_URL, AUTH):
        trigger_vdp_backup(BASE_URL, AUTH, vdp_name="sales_vdp")
