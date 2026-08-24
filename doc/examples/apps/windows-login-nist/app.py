import dash
from dash import html, dcc, dash_table, callback_context
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import os
from datetime import datetime
import time
from flask import request

app = dash.Dash(__name__)

# Initialize Databricks SDK - automatically uses app's OAuth credentials
w = WorkspaceClient()
WAREHOUSE_ID = '8dbacdcdf037944a'

print(f"Initialized WorkspaceClient for SQL warehouse: {WAREHOUSE_ID}")

def execute_query(sql_query):
    """Execute a SQL query using Databricks SDK and return results as list of tuples"""
    try:
        print(f"Executing query: {sql_query[:100]}...")
        
        # Execute the statement
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=sql_query,
            wait_timeout='50s'
        )
        
        print(f"Query state: {response.status.state}")
        
        # Check if successful
        if response.status.state == StatementState.SUCCEEDED:
            rows = []
            if response.result and response.result.data_array:
                for row_data in response.result.data_array:
                    # Convert row data to tuple
                    rows.append(tuple(row_data))
            print(f"Query returned {len(rows)} rows")
            return rows
        else:
            print(f"Query failed with state: {response.status.state}")
            if response.status.error:
                print(f"Error: {response.status.error}")
            return []
            
    except Exception as e:
        print(f"Error executing query: {e}")
        import traceback
        traceback.print_exc()
        return []

def query_login_counts():
    """Query successful and failed login counts"""
    try:
        results = execute_query("""
            SELECT 
                event_id,
                COUNT(*) as count
            FROM staging_monthly.winlog.security_demo
            WHERE event_id IN ('4624', '4625')
            GROUP BY event_id
        """)
        print(f"query_login_counts results: {results}")
        counts = {'4624': 0, '4625': 0}
        for row in results:
            print(f"Processing row: {row}, type: {type(row)}")
            counts[row[0]] = row[1]
        print(f"Final counts: {counts}")
        return counts['4624'], counts['4625']
    except Exception as e:
        print(f"Error querying login counts: {e}")
        return 0, 0

def query_logon_types():
    """Query logon type breakdown"""
    try:
        results = execute_query("""
            WITH parsed_logons AS (
              SELECT
                CAST(regexp_extract(message, 'Logon Type:\\\\s+(\\\\d+)', 1) AS INT) as logon_type,
                regexp_extract(message, 'New Logon:[\\\\s\\\\S]*?Account Name:\\\\s+([^\\\\r\\\\n]+)', 1) as account_name
              FROM staging_monthly.winlog.security_demo
              WHERE event_id = '4624'
            )
            SELECT
              logon_type,
              CASE logon_type
                WHEN 2 THEN 'Interactive (Console login)'
                WHEN 3 THEN 'Network (File share access)'
                WHEN 4 THEN 'Batch (Scheduled task)'
                WHEN 5 THEN 'Service (Windows service)'
                WHEN 7 THEN 'Unlock (Screen unlock)'
                WHEN 8 THEN 'NetworkCleartext (IIS)'
                WHEN 9 THEN 'NewCredentials (RunAs)'
                WHEN 10 THEN 'RemoteInteractive (RDP)'
                WHEN 11 THEN 'CachedInteractive (Offline)'
                ELSE 'Other/Unknown'
              END as logon_type_description,
              account_name,
              COUNT(*) as event_count
            FROM parsed_logons
            GROUP BY logon_type, account_name
            ORDER BY event_count DESC
            LIMIT 50
        """)
        return pd.DataFrame(results, columns=['Logon Type', 'Description', 'Account', 'Count'])
    except Exception as e:
        print(f"Error querying logon types: {e}")
        return pd.DataFrame(columns=['Logon Type', 'Description', 'Account', 'Count'])

def query_recent_events():
    """Query recent login events"""
    try:
        results = execute_query("""
            SELECT 
                time_created,
                event_id,
                CASE event_id 
                    WHEN '4624' THEN 'Success'
                    WHEN '4625' THEN 'Failed'
                    ELSE 'Unknown'
                END as status,
                machine_name
            FROM staging_monthly.winlog.security_demo
            WHERE event_id IN ('4624', '4625')
            ORDER BY time_created DESC
            LIMIT 20
        """)
        return pd.DataFrame(results, columns=['Time', 'Event ID', 'Status', 'Machine'])
    except Exception as e:
        print(f"Error querying recent events: {e}")
        return pd.DataFrame(columns=['Time', 'Event ID', 'Status', 'Machine'])

def query_events_over_time():
    """Query login events over time for charting"""
    try:
        results = execute_query("""
            SELECT 
                DATE_TRUNC('hour', time_created) as hour,
                event_id,
                COUNT(*) as count
            FROM staging_monthly.winlog.security_demo
            WHERE event_id IN ('4624', '4625')
            GROUP BY hour, event_id
            ORDER BY hour
        """)
        return pd.DataFrame(results, columns=['Hour', 'Event ID', 'Count'])
    except Exception as e:
        print(f"Error querying events over time: {e}")
        return pd.DataFrame(columns=['Hour', 'Event ID', 'Count'])

def query_activity_by_machine():
    """Query login activity grouped by machine"""
    try:
        results = execute_query("""
            SELECT 
                machine_name,
                COUNT(*) as login_count
            FROM staging_monthly.winlog.security_demo
            WHERE event_id IN ('4624', '4625')
            GROUP BY machine_name
            ORDER BY login_count DESC
            LIMIT 10
        """)
        return pd.DataFrame(results, columns=['Machine', 'Login Count'])
    except Exception as e:
        print(f"Error querying activity by machine: {e}")
        return pd.DataFrame(columns=['Machine', 'Login Count'])

def query_patterns_by_time():
    """Query login patterns by hour of day"""
    try:
        results = execute_query("""
            SELECT 
                HOUR(time_created) as hour_of_day,
                COUNT(*) as login_count
            FROM staging_monthly.winlog.security_demo
            WHERE event_id IN ('4624', '4625')
            GROUP BY hour_of_day
            ORDER BY hour_of_day
        """)
        return pd.DataFrame(results, columns=['Hour', 'Login Count'])
    except Exception as e:
        print(f"Error querying patterns by time: {e}")
        return pd.DataFrame(columns=['Hour', 'Login Count'])

def query_logon_type_analysis():
    """Query logon type analysis for visualization"""
    try:
        results = execute_query("""
            WITH parsed_logons AS (
              SELECT
                CAST(regexp_extract(message, 'Logon Type:\\\\s+(\\\\d+)', 1) AS INT) as logon_type,
                regexp_extract(message, 'New Logon:[\\\\s\\\\S]*?Account Name:\\\\s+([^\\\\r\\\\n]+)', 1) as account_name,
                time_created
              FROM staging_monthly.winlog.security_demo
              WHERE event_id = '4624'
                AND message IS NOT NULL
            )
            SELECT 
              COUNT(*) as event_count,
              logon_type,
              CASE logon_type
                WHEN 2 THEN 'Interactive (Console login)'
                WHEN 3 THEN 'Network (File share access)'
                WHEN 4 THEN 'Batch (Scheduled task)'
                WHEN 5 THEN 'Service (Windows service)'
                WHEN 7 THEN 'Unlock (Screen unlock)'
                WHEN 8 THEN 'NetworkCleartext (IIS)'
                WHEN 9 THEN 'NewCredentials (RunAs)'
                WHEN 10 THEN 'RemoteInteractive (RDP)'
                WHEN 11 THEN 'CachedInteractive (Offline)'
                ELSE 'Other/Unknown'
              END as logon_type_description,
              account_name,
              MIN(time_created) as first_occurrence,
              MAX(time_created) as last_occurrence
            FROM parsed_logons
            WHERE logon_type IS NOT NULL
            GROUP BY logon_type, account_name
            ORDER BY event_count DESC
            LIMIT 20
        """)
        return pd.DataFrame(results, columns=['Event Count', 'Logon Type', 'Description', 'Account Name', 'First Occurrence', 'Last Occurrence'])
    except Exception as e:
        print(f"Error querying logon type analysis: {e}")
        return pd.DataFrame(columns=['Event Count', 'Logon Type', 'Description', 'Account Name', 'First Occurrence', 'Last Occurrence'])

# App layout
app.layout = html.Div([
    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Refresh every 60 seconds
        n_intervals=0
    ),
    
    # Combined Header with NIST Controls Compliance
    html.Div([
        html.H1("Windows Login Events - NIST Controls", style={
            'textAlign': 'center',
            'color': 'white',
            'margin': '0',
            'padding': '20px 20px 10px 20px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '28px'
        }),
        html.H2("NIST Controls Compliance: Windows Login Auditing", style={
            'textAlign': 'center',
            'color': 'white',
            'margin': '0 0 15px 0',
            'padding': '0 20px',
            'fontFamily': 'Arial, sans-serif',
            'fontWeight': 'bold',
            'fontSize': '20px'
        }),
        html.P("This dashboard demonstrates compliance with key NIST 800-53 access control and audit requirements:", style={
            'color': 'white',
            'margin': '0 0 10px 0',
            'padding': '0 20px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '14px',
            'textAlign': 'left',
            'maxWidth': '1200px',
            'marginLeft': 'auto',
            'marginRight': 'auto'
        }),
        html.Ul([
            html.Li("AC-2 (Account Management): Tracks login activity by user account and machine", style={'marginBottom': '5px'}),
            html.Li("AU-2 (Audit Events): Captures all successful logon events (Event ID 4624)", style={'marginBottom': '5px'}),
            html.Li("AU-3 (Content of Audit Records): Records timestamp, machine name, event details, and full audit message", style={'marginBottom': '5px'}),
            html.Li("AU-12 (Audit Generation): Continuous monitoring with timeline analysis for anomaly detection", style={'marginBottom': '5px'}),
            html.Li("IA-2 (Identification and Authentication): Complete audit trail of authentication events", style={'marginBottom': '5px'})
        ], style={
            'color': 'white',
            'margin': '0 0 10px 0',
            'padding': '0 20px 0 60px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '13px',
            'textAlign': 'left',
            'maxWidth': '1200px',
            'marginLeft': 'auto',
            'marginRight': 'auto',
            'listStyleType': 'disc'
        }),
        html.P("Data Source: staging_monthly.winlog.security_demo | Event Focus: 4624 (Successful Logons)", style={
            'color': 'white',
            'margin': '0 0 10px 0',
            'padding': '0 20px',
            'fontFamily': 'Arial, sans-serif',
            'fontSize': '12px',
            'textAlign': 'center',
            'fontStyle': 'italic'
        }),
        html.P(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
               id='last-updated',
               style={'textAlign': 'center', 'color': 'white', 'margin': '0', 'padding': '0 20px 15px 20px', 'fontSize': '12px'})
    ], style={'backgroundColor': '#2c5f8d', 'marginBottom': '20px'}),
    
    # Main content
    html.Div([
        # Top row: Counter cards and Events chart
        html.Div([
            # Successful logins counter
            html.Div([
                html.H3("Successful Logins", style={'color': '#2ecc71', 'marginBottom': '10px', 'fontSize': '16px'}),
                html.H1(id='success-count', children='0', style={'fontSize': '48px', 'margin': '0'})
            ], style={
                'backgroundColor': 'white',
                'padding': '20px',
                'borderRadius': '10px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
                'textAlign': 'center',
                'flex': '0 0 20%',
                'marginRight': '10px'
            }),
            
            # Failed logins counter
            html.Div([
                html.H3("Failed Logins", style={'color': '#e74c3c', 'marginBottom': '10px', 'fontSize': '16px'}),
                html.H1(id='failed-count', children='0', style={'fontSize': '48px', 'margin': '0'})
            ], style={
                'backgroundColor': 'white',
                'padding': '20px',
                'borderRadius': '10px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
                'textAlign': 'center',
                'flex': '0 0 20%',
                'marginRight': '10px'
            }),
            
            # Events over time chart
            html.Div([
                html.H3("Login Events Over Time", style={'marginBottom': '15px', 'fontSize': '16px'}),
                dcc.Graph(id='events-chart', style={'height': '200px'})
            ], style={
                'backgroundColor': 'white',
                'padding': '20px',
                'borderRadius': '10px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
                'flex': '1'
            })
        ], style={'display': 'flex', 'marginBottom': '20px', 'gap': '10px'}),
        
        # Second row: Activity by Machine and Patterns by Time
        html.Div([
            # Login Activity by Machine
            html.Div([
                html.H3("Login Activity by Machine", style={'marginBottom': '15px', 'fontSize': '16px'}),
                dcc.Graph(id='machine-chart', style={'height': '200px'})
            ], style={
                'backgroundColor': 'white',
                'padding': '20px',
                'borderRadius': '10px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
                'flex': '1',
                'marginRight': '10px'
            }),
            
            # Login Patterns by Time of Day
            html.Div([
                html.H3("Login Patterns by Time of Day", style={'marginBottom': '15px', 'fontSize': '16px'}),
                dcc.Graph(id='time-pattern-chart', style={'height': '200px'})
            ], style={
                'backgroundColor': 'white',
                'padding': '20px',
                'borderRadius': '10px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
                'flex': '1'
            })
        ], style={'display': 'flex', 'marginBottom': '20px', 'gap': '10px'}),
        
        # Logon type breakdown
        html.Div([
            html.H3("Logon Type Breakdown", style={'marginBottom': '15px'}),
            html.Div(id='logon-types-table')
        ], style={
            'backgroundColor': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'marginBottom': '20px'
        }),
        
        # Recent events
        html.Div([
            html.H3("Recent Login Events", style={'marginBottom': '15px'}),
            html.Div(id='recent-events-table')
        ], style={
            'backgroundColor': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'marginBottom': '20px'
        }),
        
        # Logon Type Analysis (bottom row)
        html.Div([
            html.H3("Logon Type Analysis (What Caused Each Event)", style={'marginBottom': '15px'}),
            html.Div(id='logon-type-analysis-table')
        ], style={
            'backgroundColor': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'marginBottom': '20px'
        }),
        
        # NIST Compliance Audit Log
        html.Div([
            html.H3("NIST Compliance Audit", style={'marginBottom': '15px', 'textAlign': 'center'}),
            html.P("Click PASS or FAIL to log an audit record for all NIST controls listed above.", style={
                'textAlign': 'center',
                'marginBottom': '20px',
                'color': '#666',
                'fontSize': '14px'
            }),
            html.Div([
                html.Div(
                    dcc.ConfirmDialogProvider(
                        children=html.Button('PASS', style={
                            'backgroundColor': '#2ecc71',
                            'color': 'white',
                            'border': 'none',
                            'padding': '15px 40px',
                            'fontSize': '18px',
                            'fontWeight': 'bold',
                            'borderRadius': '5px',
                            'cursor': 'pointer',
                            'boxShadow': '0 2px 4px rgba(0,0,0,0.2)'
                        }),
                        id='audit-pass-confirm',
                        message='Are you sure you want to log this audit as PASSED for all 5 NIST controls (AC-2, AU-2, AU-3, AU-12, IA-2)?'
                    ),
                    style={'display': 'inline-block', 'marginRight': '15px'}
                ),
                html.Div(
                    dcc.ConfirmDialogProvider(
                        children=html.Button('FAIL', style={
                            'backgroundColor': '#e74c3c',
                            'color': 'white',
                            'border': 'none',
                            'padding': '15px 40px',
                            'fontSize': '18px',
                            'fontWeight': 'bold',
                            'borderRadius': '5px',
                            'cursor': 'pointer',
                            'boxShadow': '0 2px 4px rgba(0,0,0,0.2)'
                        }),
                        id='audit-fail-confirm',
                        message='Are you sure you want to log this audit as FAILED for all 5 NIST controls (AC-2, AU-2, AU-3, AU-12, IA-2)?'
                    ),
                    style={'display': 'inline-block'}
                )
            ], style={'textAlign': 'center', 'marginBottom': '15px'}),
            html.Div(id='audit-confirmation', style={
                'textAlign': 'center',
                'marginTop': '15px',
                'fontSize': '14px',
                'fontWeight': 'bold'
            })
        ], style={
            'backgroundColor': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
            'marginBottom': '20px'
        }),
        
        # Audit History Table
        html.Div([
            html.H3("Audit History", style={'marginBottom': '15px'}),
            html.Div(id='audit-history-table')
        ], style={
            'backgroundColor': 'white',
            'padding': '20px',
            'borderRadius': '10px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        })
    ], style={'maxWidth': '1400px', 'margin': '0 auto', 'padding': '20px'})
], style={'backgroundColor': '#f5f5f5', 'minHeight': '100vh', 'fontFamily': 'Arial, sans-serif'})

# Callbacks to update data
@app.callback(
    [Output('success-count', 'children'),
     Output('failed-count', 'children'),
     Output('events-chart', 'figure'),
     Output('machine-chart', 'figure'),
     Output('time-pattern-chart', 'figure'),
     Output('logon-type-analysis-table', 'children'),
     Output('logon-types-table', 'children'),
     Output('recent-events-table', 'children'),
     Output('audit-history-table', 'children'),
     Output('last-updated', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    # Query all data
    success_count, failed_count = query_login_counts()
    logon_types_df = query_logon_types()
    recent_events_df = query_recent_events()
    events_time_df = query_events_over_time()
    activity_by_machine_df = query_activity_by_machine()
    time_pattern_df = query_patterns_by_time()
    logon_type_analysis_df = query_logon_type_analysis()
    
    # Create time series chart
    fig = go.Figure()
    
    if not events_time_df.empty:
        for event_id in events_time_df['Event ID'].unique():
            data = events_time_df[events_time_df['Event ID'] == event_id]
            fig.add_trace(go.Scatter(
                x=data['Hour'],
                y=data['Count'],
                mode='lines+markers',
                name='Successful' if event_id == '4624' else 'Failed',
                line=dict(color='#2ecc71' if event_id == '4624' else '#e74c3c')
            ))
    
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Count",
        hovermode='x unified',
        plot_bgcolor='white',
        height=220,
        margin=dict(l=40, r=20, t=20, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    # Create machine activity chart
    machine_fig = go.Figure()
    if not activity_by_machine_df.empty:
        machine_fig.add_trace(go.Bar(
            x=activity_by_machine_df['Machine'],
            y=pd.to_numeric(activity_by_machine_df['Login Count'], errors='coerce'),
            marker=dict(color='#3498db'),
            width=0.6
        ))
    machine_fig.update_layout(
        xaxis_title="Machine",
        yaxis_title="Login Count",
        plot_bgcolor='white',
        height=220,
        margin=dict(l=40, r=20, t=20, b=40),
        showlegend=False,
        xaxis=dict(type='category'),
        yaxis=dict(type='linear')
    )
    
    # Create time pattern chart
    time_pattern_fig = go.Figure()
    if not time_pattern_df.empty:
        time_pattern_fig.add_trace(go.Bar(
            x=pd.to_numeric(time_pattern_df['Hour'], errors='coerce'),
            y=pd.to_numeric(time_pattern_df['Login Count'], errors='coerce'),
            marker=dict(color='#9b59b6'),
            width=0.8
        ))
    time_pattern_fig.update_layout(
        xaxis_title="Hour of Day",
        yaxis_title="Login Count",
        plot_bgcolor='white',
        height=220,
        margin=dict(l=40, r=20, t=20, b=40),
        showlegend=False,
        xaxis=dict(tickmode='linear', tick0=0, dtick=2, type='linear'),
        yaxis=dict(type='linear')
    )
    
    # Create logon types table
    logon_types_table = dash_table.DataTable(
        data=logon_types_df.to_dict('records') if not logon_types_df.empty else [],
        columns=[{"name": i, "id": i} for i in logon_types_df.columns] if not logon_types_df.empty else [],
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={
            'backgroundColor': '#1e3a5f',
            'color': 'white',
            'fontWeight': 'bold'
        },
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}
        ],
        page_size=10
    )
    
    # Create recent events table
    recent_events_table = dash_table.DataTable(
        data=recent_events_df.to_dict('records') if not recent_events_df.empty else [],
        columns=[{"name": i, "id": i} for i in recent_events_df.columns] if not recent_events_df.empty else [],
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={
            'backgroundColor': '#1e3a5f',
            'color': 'white',
            'fontWeight': 'bold'
        },
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'},
            {'if': {'filter_query': '{Status} = "Failed"', 'column_id': 'Status'},
             'backgroundColor': '#ffe5e5', 'color': '#e74c3c', 'fontWeight': 'bold'},
            {'if': {'filter_query': '{Status} = "Success"', 'column_id': 'Status'},
             'backgroundColor': '#e5ffe5', 'color': '#2ecc71', 'fontWeight': 'bold'}
        ],
        page_size=10
    )
    
    # Create logon type analysis table
    logon_type_analysis_table = dash_table.DataTable(
        data=logon_type_analysis_df.to_dict('records') if not logon_type_analysis_df.empty else [],
        columns=[{"name": i, "id": i} for i in logon_type_analysis_df.columns] if not logon_type_analysis_df.empty else [],
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={
            'backgroundColor': '#1e3a5f',
            'color': 'white',
            'fontWeight': 'bold'
        },
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}
        ],
        page_size=10
    )
    
    # Query audit history
    audit_history_query = """
        SELECT 
            audit_timestamp as `Audit Timestamp`,
            nist_control as `NIST Control`,
            status as `Status`,
            auditor as `Auditor`
        FROM staging_monthly.winlog.nist_audit_log
        ORDER BY audit_timestamp DESC
        LIMIT 50
    """
    audit_history_results = execute_query(audit_history_query)
    audit_history_df = pd.DataFrame(audit_history_results, columns=['Audit Timestamp', 'NIST Control', 'Status', 'Auditor']) if audit_history_results else pd.DataFrame(columns=['Audit Timestamp', 'NIST Control', 'Status', 'Auditor'])
    
    # Create audit history table
    has_audit_data = not audit_history_df.empty
    audit_history_table = dash_table.DataTable(
        data=audit_history_df.to_dict('records') if has_audit_data else [],
        columns=[{"name": i, "id": i} for i in audit_history_df.columns] if has_audit_data else [],
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={
            'backgroundColor': '#1e3a5f',
            'color': 'white',
            'fontWeight': 'bold'
        },
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'},
            {'if': {'filter_query': '{Status} = "PASS"', 'column_id': 'Status'},
             'backgroundColor': '#e5ffe5', 'color': '#2ecc71', 'fontWeight': 'bold'},
            {'if': {'filter_query': '{Status} = "FAIL"', 'column_id': 'Status'},
             'backgroundColor': '#ffe5e5', 'color': '#e74c3c', 'fontWeight': 'bold'}
        ],
        page_size=10
    )
    
    last_updated = f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    return success_count, failed_count, fig, machine_fig, time_pattern_fig, logon_types_table, recent_events_table, logon_type_analysis_table, audit_history_table, last_updated

# Callback for NIST audit logging
@app.callback(
    Output('audit-confirmation', 'children'),
    [Input('audit-pass-confirm', 'submit_n_clicks'),
     Input('audit-fail-confirm', 'submit_n_clicks')]
)
def log_audit(pass_clicks, fail_clicks):
    """Log NIST compliance audit to database"""
    ctx = callback_context
    
    # Don't do anything on initial load
    if not ctx.triggered:
        return ''
    
    # Determine which button was clicked
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if button_id == 'audit-pass-confirm':
        status = 'PASS'
        color = '#2ecc71'
    elif button_id == 'audit-fail-confirm':
        status = 'FAIL'
        color = '#e74c3c'
    else:
        return ''
    
    try:
        # Get current user email from request headers (set by Databricks Apps OAuth)
        auditor = request.headers.get('X-Forwarded-Email', 'unknown')
        
        if auditor == 'unknown':
            # Fallback to trying the WorkspaceClient
            try:
                current_user = w.current_user.me()
                auditor = current_user.user_name
            except Exception as e:
                auditor = 'unknown'
        
        # Get current timestamp
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # NIST controls to log
        nist_controls = ['AC-2', 'AU-2', 'AU-3', 'AU-12', 'IA-2']
        
        # Insert a record for each control
        for control in nist_controls:
            insert_query = f"""
                INSERT INTO staging_monthly.winlog.nist_audit_log 
                (audit_timestamp, nist_control, status, auditor)
                VALUES ('{current_time}', '{control}', '{status}', '{auditor}')
            """
            execute_query(insert_query)
        
        return html.Div([
            html.Span('✓ ', style={'fontSize': '20px'}),
            f"Audit logged as {status} for all 5 NIST controls by {auditor} at {current_time}"
        ], style={'color': color})
        
    except Exception as e:
        return html.Div(f"Error logging audit: {str(e)}", style={'color': '#e74c3c'})

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8000, debug=False)