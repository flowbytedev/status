import os
import pandas as pd
import numpy as np
import subprocess
import requests
import json
#import polars as pl
import pyodbc
from dagster import MetadataValue, Output, asset, MonthlyPartitionsDefinition, run_status_sensor, DagsterRunStatus, DefaultSensorStatus, RunStatusSensorContext, DagsterEventType
from datetime import datetime, timedelta
import logging as logger
import pyarrow as pa
from datetime import datetime
from sqlalchemy import Column, DECIMAL
from sqlalchemy.ext.declarative import declarative_base
import pyodbc




def post_status_history(context, entity_id, status_code, status_message, checked_at):
    """
    Post status history to the STATUSAPP API
    """
    try:
        status_payload = {
            "EntityId": entity_id,
            "Status": status_code,
            "StatusMessage": status_message,
            "CheckedAt": checked_at,
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        status_api_url = os.getenv("STATUSAPP_API_URL")
        
        # Verify payload structure
        context.log.info(f"[STATUS API] ===== PAYLOAD VERIFICATION =====")
        context.log.info(f"[STATUS API] Sending to URL: {status_api_url}")
        context.log.info(f"[STATUS API] Payload keys: {list(status_payload.keys())}")
        context.log.info(f"[STATUS API] Expected keys: ['EntityId', 'Status', 'StatusMessage', 'CheckedAt']")
        context.log.info(f"[STATUS API] Payload verification: {set(status_payload.keys()) == {'EntityId', 'Status', 'StatusMessage', 'CheckedAt'}}")
        context.log.info(f"[STATUS API] Full Payload: {json.dumps(status_payload, indent=2)}")
        context.log.info(f"[STATUS API] Headers: {json.dumps(headers, indent=2)}")
        
        if status_api_url:
            resp = requests.post(
                status_api_url,
                data=json.dumps(status_payload),
                headers=headers,
                verify=False,
                timeout=30
            )
            
            # Detailed response logging
            success = resp.status_code == 200
            context.log.info(f"[STATUS API] ===== RESPONSE DETAILS =====")
            context.log.info(f"[STATUS API] Response Status: HTTP {resp.status_code}")
            context.log.info(f"[STATUS API] Success: {success}")
            context.log.info(f"[STATUS API] Response Body: {resp.text}")
            context.log.info(f"[STATUS API] Response Headers: {dict(resp.headers)}")
            
            if success:
                context.log.info(f"[STATUS API] ✅ POST SUCCESSFUL - Status history posted for EntityId {entity_id}")
            else:
                context.log.error(f"[STATUS API] ❌ POST FAILED - HTTP {resp.status_code} for EntityId {entity_id}")
            
            context.log.info(f"[STATUS API] =====================================")
            return success
        else:
            context.log.warning("[STATUS API] ❌ STATUSAPP_API_URL not configured, skipping status update")
            return False
    except Exception as e:
        context.log.error(f"[STATUS API] ❌ EXCEPTION - Failed to post status history for EntityId {entity_id}: {e}")
        return False


def create_incident(context, entity_id, title, description, severity=4):
    """
    Create an incident via the INCIDENT API
    """
    try:
        incident_payload = {
            "EntityId": entity_id,
            "Title": title,
            "Description": description[:4000],  # Truncate long error messages
            "Severity": severity,
            "Status": 1,
            "StartedAt": datetime.utcnow().isoformat() + "Z",
            "ReportedBy": "Dagster Job"
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        incident_api_url = os.getenv("INCIDENT_API_URL")
        
        # Verify payload structure
        context.log.info(f"[INCIDENT API] ===== PAYLOAD VERIFICATION =====")
        context.log.info(f"[INCIDENT API] Sending to URL: {incident_api_url}")
        context.log.info(f"[INCIDENT API] Payload keys: {list(incident_payload.keys())}")
        context.log.info(f"[INCIDENT API] Expected keys: ['EntityId', 'Title', 'Description', 'Severity', 'Status', 'StartedAt', 'ReportedBy']")
        expected_keys = {'EntityId', 'Title', 'Description', 'Severity', 'Status', 'StartedAt', 'ReportedBy'}
        context.log.info(f"[INCIDENT API] Payload verification: {set(incident_payload.keys()) == expected_keys}")
        context.log.info(f"[INCIDENT API] Full Payload: {json.dumps(incident_payload, indent=2)}")
        context.log.info(f"[INCIDENT API] Headers: {json.dumps(headers, indent=2)}")
        
        if incident_api_url:
            resp = requests.post(
                incident_api_url,
                data=json.dumps(incident_payload),
                headers=headers,
                verify=False,
                timeout=30
            )
            
            # Detailed response logging
            success = resp.status_code == 200
            context.log.info(f"[INCIDENT API] ===== RESPONSE DETAILS =====")
            context.log.info(f"[INCIDENT API] Response Status: HTTP {resp.status_code}")
            context.log.info(f"[INCIDENT API] Success: {success}")
            context.log.info(f"[INCIDENT API] Response Body: {resp.text}")
            context.log.info(f"[INCIDENT API] Response Headers: {dict(resp.headers)}")
            
            if success:
                context.log.info(f"[INCIDENT API] ✅ POST SUCCESSFUL - Incident created for EntityId {entity_id}")
            else:
                context.log.error(f"[INCIDENT API] ❌ POST FAILED - HTTP {resp.status_code} for EntityId {entity_id}")
            
            context.log.info(f"[INCIDENT API] =====================================")
            return success
        else:
            context.log.warning("[INCIDENT API] ❌ INCIDENT_API_URL not configured, skipping incident creation")
            return False
    except Exception as e:
        context.log.error(f"[INCIDENT API] ❌ EXCEPTION - Failed to create incident for EntityId {entity_id}: {e}")
        return False






@asset(owners=["peter.elhage@spinneys-lebanon.com", "team:data-eng"], compute_kind="sql", group_name="source")
def get_server_details(context):
    """
    Get server details from STATUSAPP database where entity_type = 1
    and ping each server to check connectivity
    """
    
    # Database connection parameters from environment
    server = os.getenv('STATUSAPP_SERVER')
    database = os.getenv('STATUSAPP_DATABASE')
    username = os.getenv('STATUSAPP_USER')
    password = os.getenv('STATUSAPP_PASSWORD')
    
    # Create connection string
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    try:
        # Connect to database and get servers
        with pyodbc.connect(connection_string) as conn:
            query = """
            SELECT 
                id,
                name
            FROM Entity 
            WHERE entity_type = 1
            """
            
            # Execute query and get data
            servers_df = pd.read_sql(query, conn)
            
        context.log.info(f"Retrieved {len(servers_df)} servers from database")
        
        # Log the API URLs being used
        status_api_url = os.getenv("STATUSAPP_API_URL")
        incident_api_url = os.getenv("INCIDENT_API_URL")
        context.log.info(f"[CONFIG] Status API URL: {status_api_url}")
        context.log.info(f"[CONFIG] Incident API URL: {incident_api_url}")
        
        # Log the servers we found
        context.log.info(f"[SERVERS] Found servers:")
        for index, row in servers_df.iterrows():
            context.log.info(f"[SERVERS] ID: {row['id']}, Name: {row['name']}")
        
        # Ping each server
        ping_results = []
        
        for index, row in servers_df.iterrows():
            server_id = row['id']
            server_name = row['name']
            
            # Since we don't have ip_address in the query, we'll use the name as IP
            # You may need to adjust this based on your data structure
            ip_address = server_name  # Assuming name contains IP or hostname
            port = None  # No port information available
            
            context.log.info(f"[PING] Starting ping test for server {server_id}: {server_name} -> {ip_address}")
            
            try:
                # Ping the server using system ping command
                import subprocess
                
                # For Windows, use ping command
                ping_command = f"ping -n 1 -w 1000 {ip_address}"
                context.log.info(f"[PING] Executing command: {ping_command}")
                
                result = subprocess.run(ping_command, shell=True, capture_output=True, text=True)
                
                # Check if ping was successful
                ping_success = result.returncode == 0
                
                context.log.info(f"[PING] Command return code: {result.returncode}")
                context.log.info(f"[PING] Command stdout: {result.stdout}")
                if result.stderr:
                    context.log.info(f"[PING] Command stderr: {result.stderr}")
                
                # Extract response time if available
                response_time = None
                if ping_success and "time=" in result.stdout:
                    try:
                        # Extract time from ping output (e.g., "time=1ms")
                        time_part = result.stdout.split("time=")[1].split("ms")[0]
                        response_time = float(time_part.replace("<", ""))
                        context.log.info(f"[PING] Extracted response time: {response_time}ms")
                    except:
                        response_time = None
                        context.log.warning(f"[PING] Could not extract response time from: {result.stdout}")
                
                # Determine status code and message
                if ping_success:
                    status_code = 1
                    status_message = "The server is online"
                else:
                    status_code = 2
                    status_message = "The server is offline"
                
                context.log.info(f"[PING] Result: {status_message} (Status Code: {status_code})")
                
                # Current timestamp for the check
                checked_at = datetime.utcnow().isoformat() + "Z"
                
                # Post status history
                context.log.info(f"[PING] Posting status history for server {server_id}")
                status_posted = post_status_history(context, server_id, status_code, status_message, checked_at)
                
                # Create incident if server is offline
                if not ping_success:
                    context.log.info(f"[PING] Creating incident for offline server {server_id}")
                    incident_created = create_incident(
                        context,
                        entity_id=server_id,
                        title=f"Server '{server_name}' is offline",
                        description=f"Ping to server {server_name} ({ip_address}) failed. Error: {result.stderr or 'Ping timeout or unreachable'}",
                        severity=4
                    )
                    context.log.info(f"[PING] Incident creation result: {'Success' if incident_created else 'Failed'}")
                
                ping_results.append({
                    'server_id': server_id,
                    'server_name': server_name,
                    'ip_address': ip_address,
                    'port': port,
                    'ping_success': ping_success,
                    'response_time_ms': response_time,
                    'ping_timestamp': datetime.now(),
                    'error_message': None if ping_success else result.stderr or "Ping failed",
                    'status_code': status_code,
                    'status_message': status_message
                })
                
                context.log.info(f"[PING] Completed ping test for {server_name} ({ip_address}): {'SUCCESS' if ping_success else 'FAILED'}")
                
            except Exception as e:
                context.log.error(f"[PING] Exception during ping test for server {server_id}: {str(e)}")
                
                # Handle ping exception
                status_code = 2
                status_message = "The server is offline"
                checked_at = datetime.utcnow().isoformat() + "Z"
                
                # Post status history for error case
                context.log.info(f"[PING] Posting status history for error case - server {server_id}")
                status_posted = post_status_history(context, server_id, status_code, status_message, checked_at)
                
                # Create incident for error case
                context.log.info(f"[PING] Creating incident for error case - server {server_id}")
                incident_created = create_incident(
                    context,
                    entity_id=server_id,
                    title=f"Server '{server_name}' ping error",
                    description=f"Error while pinging server {server_name} ({ip_address}): {str(e)}",
                    severity=4
                )
                
                ping_results.append({
                    'server_id': server_id,
                    'server_name': server_name,
                    'ip_address': ip_address,
                    'port': port,
                    'ping_success': False,
                    'response_time_ms': None,
                    'ping_timestamp': datetime.now(),
                    'error_message': str(e),
                    'status_code': status_code,
                    'status_message': status_message
                })
                
                context.log.error(f"[PING] Error pinging {server_name} ({ip_address}): {str(e)}")
            
            context.log.info(f"[PING] ==========================================")
        
        # Convert ping results to DataFrame
        ping_df = pd.DataFrame(ping_results)
        
        # Merge server details with ping results
        final_df = servers_df.merge(
            ping_df[['server_id', 'ping_success', 'response_time_ms', 'ping_timestamp', 'error_message', 'status_code', 'status_message']], 
            left_on='id', 
            right_on='server_id', 
            how='left'
        ).drop('server_id', axis=1)
        
        # Calculate summary statistics
        successful_pings = sum(1 for result in ping_results if result['ping_success'])
        failed_pings = sum(1 for result in ping_results if not result['ping_success'])
        avg_response_time = float(np.mean([r['response_time_ms'] for r in ping_results if r['response_time_ms'] is not None])) if any(r['response_time_ms'] for r in ping_results) else None
        
        # Log comprehensive summary
        context.log.info(f"[SUMMARY] ==========================================")
        context.log.info(f"[SUMMARY] Ping Monitoring Job Complete")
        context.log.info(f"[SUMMARY] Total servers processed: {len(servers_df)}")
        context.log.info(f"[SUMMARY] Successful pings: {successful_pings}")
        context.log.info(f"[SUMMARY] Failed pings: {failed_pings}")
        if avg_response_time:
            context.log.info(f"[SUMMARY] Average response time: {avg_response_time:.2f}ms")
        else:
            context.log.info(f"[SUMMARY] Average response time: N/A (no successful pings)")
        
        # Log details for each server
        context.log.info(f"[SUMMARY] Individual server results:")
        for result in ping_results:
            status = "ONLINE" if result['ping_success'] else "OFFLINE"
            response_time = f"{result['response_time_ms']}ms" if result['response_time_ms'] else "N/A"
            context.log.info(f"[SUMMARY]   {result['server_name']} ({result['ip_address']}): {status} - Response: {response_time}")
        
        context.log.info(f"[SUMMARY] Final DataFrame shape: {final_df.shape}")
        context.log.info(f"[SUMMARY] Final DataFrame columns: {list(final_df.columns)}")
        context.log.info(f"[SUMMARY] ==========================================")
        
        context.log.info(f"Completed ping test for {len(ping_results)} servers")
        
        # Return the result
        return Output(
            final_df,
            metadata={
                "num_servers": len(servers_df),
                "successful_pings": sum(1 for result in ping_results if result['ping_success']),
                "failed_pings": sum(1 for result in ping_results if not result['ping_success']),
                "avg_response_time": float(np.mean([r['response_time_ms'] for r in ping_results if r['response_time_ms'] is not None])) if any(r['response_time_ms'] for r in ping_results) else None
            }
        )
        
    except Exception as e:
        context.log.error(f"Error connecting to database or executing query: {str(e)}")
        raise e