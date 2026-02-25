import streamlit as st
import pandas as pd
import requests
import json
import os
import re
from datetime import datetime, date
from dotenv import load_dotenv
from groq import Groq

# Load environment variables (for local runs)
load_dotenv()

def get_secret(key, default=""):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

MONDAY_API_KEY = get_secret("MONDAY_API_KEY", "")
GROQ_API_KEY = get_secret("GROQ_API_KEY", "")
DEALS_BOARD_ID = get_secret("DEALS_BOARD_ID", "")
WORK_ORDERS_BOARD_ID = get_secret("WORK_ORDERS_BOARD_ID", "")

# =========================================================
# 1. Monday.com Integration
# =========================================================
class MondayConnector:
    """Connects to monday.com via API, handles auth and connection management."""
    def __init__(self, api_key):
        self.api_key = api_key
        self.url = "https://api.monday.com/v2/"
        self.headers = {
            "Authorization": self.api_key,
            "API-Version": "2024-01",
            "Content-Type": "application/json"
        }

    def fetch_board_data(self, board_id):
        if not self.api_key or not board_id:
            return {"error": "Monday API Key or Board ID not configured."}
        
        query = """
        query ($boardId: [ID!]) { 
            boards (ids: $boardId) { 
                name
                items_page (limit: 500) {
                    cursor
                    items {
                        name
                        column_values {
                            id
                            text
                            value
                            column {
                                title
                            }
                        }
                    }
                } 
            } 
        }
        """
        
        payload = {
            "query": query,
            "variables": {"boardId": [int(board_id)]} if str(board_id).isdigit() else {"boardId": [board_id]}
        }
        
        try:
            response = requests.post(self.url, headers=self.headers, json=payload)
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                return {"error": f"GraphQL Error: {data['errors']}"}
            return data
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    def extract_items(self, monday_json):
        if "error" in monday_json:
            return []
        try:
            boards = monday_json.get("data", {}).get("boards", [])
            if not boards:
                return []
            items = boards[0].get("items_page", {}).get("items", [])
            extracted_items = []
            for item in items:
                row = {"Deal Name": item.get("name")}
                for col in item.get("column_values", []):
                    title = col.get("column", {}).get("title", col.get("id"))
                    text_val = col.get("text", "")
                    # Prefer text, if empty, it might be null
                    row[title] = text_val.strip() if text_val else None
                extracted_items.append(row)
            return extracted_items
        except Exception as e:
            return []

# =========================================================
# 2. Data Resilience
# =========================================================
class DataResilience:
    """Handles missing values, normalizes dates/sectors, tracks quality issues."""
    def __init__(self):
        self.data_quality_notes = []

    def get_start_of_current_quarter(self):
        current_month = datetime.now().month
        current_year = datetime.now().year
        quarter_start_month = 3 * ((current_month - 1) // 3) + 1
        return date(current_year, quarter_start_month, 1).strftime('%Y-%m-%d')

    def normalize_sector(self, sector_str):
        if not sector_str or pd.isna(sector_str):
            return "Unknown"
        s = str(sector_str).lower().strip()
        if re.search(r'energy|enrg', s): return "Energy"
        if re.search(r'min(ing|e)', s): return "Mining"
        if re.search(r'power(line)?', s): return "Powerline"
        if re.search(r'rail(way)?s?', s): return "Railways"
        if re.search(r'const(ruction)?', s): return "Construction"
        return sector_str.title()

    def parse_amount(self, amount_str):
        if not amount_str or pd.isna(amount_str):
            return None
        # Remove anything that isn't a digit or a period
        cleaned = re.sub(r'[^\d.]', '', str(amount_str))
        if not cleaned: return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    def clean_deals(self, raw_deals):
        df = pd.DataFrame(raw_deals)
        if df.empty: 
            self.data_quality_notes.append("CRITICAL: No deals found in the specified Pipeline board.")
            return df
        
        # Determine the logical columns based on what's available
        possible_value_cols = [c for c in df.columns if "value" in c.lower() or "amount" in c.lower()]
        possible_sector_cols = [c for c in df.columns if "sector" in c.lower() or "service" in c.lower()]
        possible_status_cols = [c for c in df.columns if "status" in c.lower() or "stage" in c.lower()]
        possible_date_cols = [c for c in df.columns if "date" in c.lower()]

        # Standardize Names Internally if they exist
        if possible_value_cols: df = df.rename(columns={possible_value_cols[0]: "Deal_Value"})
        if possible_sector_cols: df = df.rename(columns={possible_sector_cols[0]: "Sector_Norm"})
        if possible_status_cols: df = df.rename(columns={possible_status_cols[0]: "Status_Norm"})
        
        # 2A. Normalize Sectors
        if "Sector_Norm" in df.columns:
            df["Sector_Norm"] = df["Sector_Norm"].apply(self.normalize_sector)
        else:
            df["Sector_Norm"] = "Unknown Sector"

        # 2B. Handle Missing Financial Data Gracefully
        if "Deal_Value" in df.columns:
            df["Deal_Value"] = df["Deal_Value"].apply(self.parse_amount)
            missing = df["Deal_Value"].isna().sum()
            
            if missing > 0:
                self.data_quality_notes.append(f"Resilience Note: {missing} deals had missing or unparseable financial values. Defaulted them to 0.0 to prevent calculation failure.")
                df["Deal_Value"] = df["Deal_Value"].fillna(0.0)
        else:
            self.data_quality_notes.append("CRITICAL: Could not find any column representing Deal Value.")
            df["Deal_Value"] = 0.0

        # 2C. Normalize Dates
        if possible_date_cols:
            primary_date = possible_date_cols[0]
            def fix_date(d):
                if pd.isna(d): return None
                try: return pd.to_datetime(d).strftime('%Y-%m-%d')
                except: return None
            
            df["Date_Norm"] = df[primary_date].apply(fix_date)
            missing_dates = df["Date_Norm"].isna().sum()
            if missing_dates > 0:
                self.data_quality_notes.append(f"Resilience Note: {missing_dates} dates in '{primary_date}' were missing/invalid. Defaulting to start of current quarter.")
                df["Date_Norm"] = df["Date_Norm"].fillna(self.get_start_of_current_quarter())
                
        return df

    def clean_work_orders(self, raw_wo):
        df = pd.DataFrame(raw_wo)
        if df.empty:
            self.data_quality_notes.append("CRITICAL: No work orders found in the Operations board.")
            return df
            
        # Normalize core operational columns
        possible_status = [c for c in df.columns if "status" in c.lower() or "execution" in c.lower() or "proof" in c.lower()]
        
        if possible_status:
            stat_col = possible_status[0]
            df["Status_Norm"] = df[stat_col].astype(str).str.lower().str.strip()
        else:
            df["Status_Norm"] = "unknown"
            self.data_quality_notes.append("Warning: Could not identify a Status column in the Work Orders board.")
            
        return df

# =========================================================
# 3 & 4. Query Understanding & Business Intelligence
# =========================================================
class BusinessIntelligence:
    """Answers queries about pipeline health, sectoral performance, and operational metrics."""
    
    def calculate_pipeline_metrics(self, df_deals):
        if df_deals.empty: return {"Total Pipeline": 0, "Won Deals Value": 0}
        
        metrics = {}
        if "Status_Norm" in df_deals.columns:
            # Deals that are NOT won/completed
            active_mask = ~df_deals["Status_Norm"].str.lower().str.contains("won|complet|success", na=False)
            metrics["Active Pipeline Value"] = df_deals.loc[active_mask, "Deal_Value"].sum()
            metrics["Won Deals Value"] = df_deals.loc[~active_mask, "Deal_Value"].sum()
        else:
            metrics["Raw Total Deals Found (Unfiltered)"] = df_deals["Deal_Value"].sum()
            
        return metrics
        
    def calculate_sector_health(self, df_deals):
        if df_deals.empty: return {}
        return df_deals.groupby("Sector_Norm")["Deal_Value"].sum().to_dict()
        
    def cross_board_operational_risk(self, df_deals, df_wo):
        """Cross-Board Querying: Identify 'Won' sales that have no started operation."""
        if df_deals.empty or df_wo.empty:
            return "Cannot perform cross-board analysis due to missing data in one or both boards."
            
        if "Status_Norm" not in df_deals.columns:
            return "Cannot identify won deals because status column is missing."
            
        # Identify Won Sales
        won_deals = df_deals[df_deals["Status_Norm"].str.lower().str.contains("won|complet|success", na=False)]
        
        if won_deals.empty:
            return "Analysis Complete: No 'Won' deals discovered in the pipeline to cross-check against operations."
            
        wo_names = df_wo["Deal Name"].str.lower().str.strip().tolist() if "Deal Name" in df_wo.columns else []
        
        # Check Operations Board for these won deals
        bottlenecks = []
        for _, deal in won_deals.iterrows():
            name = str(deal.get("Deal Name", "")).lower().strip()
            if not name or name == "none": continue
            
            # Entity Matching Strategy
            match = df_wo[df_wo["Deal Name"].str.lower().str.strip() == name] if "Deal Name" in df_wo.columns else pd.DataFrame()
            
            if match.empty:
                bottlenecks.append(f"'{deal.get('Deal Name')}' (Ghost Deal - Sold but missing from Operations)")
            else:
                op_status = str(match["Status_Norm"].iloc[0])
                if "not started" in op_status or "pending" in op_status or "delayed" in op_status:
                    bottlenecks.append(f"'{deal.get('Deal Name')}' (Bottleneck - Sold but Operations status is {op_status})")
                    
        return {
            "Total Won Sales Tracked": len(won_deals),
            "Bottlenecks Identified": len(bottlenecks),
            "Specific Bottlenecks": bottlenecks if bottlenecks else "Operations are cleanly executing won deals."
        }

    def calculate_deal_stage_funnel(self, df_deals):
        if df_deals.empty: return {}
        status_col = "Deal Status" if "Deal Status" in df_deals.columns else "Deal Stage"
        if status_col in df_deals.columns:
            return df_deals[status_col].value_counts().to_dict()
        return {}

# =========================================================
# 5. Response Manager (Interpretation & Delivery)
# =========================================================
class ResponseManager:
    """Interprets founder questions, asks clarifying questions, outputs insights."""
    def __init__(self):
        self.client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

    def run_agent(self, user_prompt, chat_history, context_data, quality_notes):
        if not self.client:
            return "Groq API Key is not configured."
            
        notes_str = "\n".join([f"- {note}" for note in quality_notes])
        if not notes_str: notes_str = "Data appears fully robust with no identified nulls or parsing errors."
        
        system_prompt = f"""# ROLE & DIRECTIVE
You are a Senior Business Intelligence Advisor for Founders. 
Your primary task is QUERY UNDERSTANDING and INSIGHT GENERATION.

# 3. QUERY UNDERSTANDING RULES
- Interpret founder-speak. If they ask "Are we oversold?", they are asking for the Cross-Board Operational Risk analysis.
- If the founder's query is too vague, actively ask targeted clarifying questions.

# 4. BUSINESS INTELLIGENCE RULES
- Provide STRAIGHT TO THE POINT, FACTUAL answers. 
- DO NOT provide summaries, extra context, fluff, or "What next?" details UNLESS the user explicitly asks you to summarize or explain.
- If a Vice President asks a specific question (e.g., "How's the energy sector pipeline?"), give them ONLY the exact numbers and facts directly answering that question.
- Format numbers as readable currencies (e.g., $1.2M rather than 1200000).

# 2. DATA RESILIENCE RULES (STRICT FLAG)
A data resilience and normalizer engine has run before you. It left these Data Quality Notes:
{notes_str}
You MUST include a "Data Quality Caveats" section at the end of your response summarizing these notes so the founder knows if the data is incomplete.

# VISUALIZATION PROTOCOL
If the user asks to see a chart, graph, dashboard, or explicitly asks to "visualize" the data, you can choose to display one or more of the following charts by including their exact tags in your response. **Only include charts that are directly relevant to the user's question!** You can include multiple tags.
Available chart tags:
- `[CHART: SECTOR_PIE]` : Shows Revenue by Sector breakdown.
- `[CHART: PIPELINE_BAR]` : Shows Active Pipeline Value vs Won Deals Value.
- `[CHART: BOTTLENECK_BAR]` : Shows a count of won deals vs oversold bottlenecks (operations lagging).
- `[CHART: DEAL_STAGE_FUNNEL]` : Shows a funnel of deals by their current stage.

# PRE-AGGREGATED CONTEXT
The BusinessIntelligence engine has run across both boards and provided this snapshot explicitly for your use:
{context_data}
"""
        messages = [{"role": "system", "content": system_prompt}]
        for msg in chat_history[-4:]: # Keep context window clean
            messages.append({"role": msg["role"], "content": msg["content"]})
            
        messages.append({"role": "user", "content": user_prompt})
        
        try:
            # Using the fast llama 3.1 8b instant model
            response = self.client.chat.completions.create(model="llama-3.1-8b-instant", messages=messages, temperature=0.2)
            return response.choices[0].message.content
        except Exception as e:
            return f"Error interacting with LLM API: {str(e)}"

# =========================================================
# Streamlit Interface
# =========================================================
st.set_page_config(page_title="Executive BI Agent", page_icon="📊", layout="wide")
st.title("Executive Intelligence Agent")
st.markdown("Powered by Groq, Monday.com, and strict Data Resilience Strategies.")

# Apply environment configurations automatically
# MONDAY_API_KEY, GROQ_API_KEY, etc. are loaded at the top from st.secrets / .env

if "messages" not in st.session_state:
    st.session_state.messages = []

with st.sidebar:
    st.header("🛠️ Dashboard Controls")
    st.success("Connected via Environment Secrets")
    if st.button("Reset Session / Clear Chat"):
        st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Ask a business question... (e.g. 'Are we oversold?')"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Executing Data Resilience Protocol & Cross-Board Queries..."):
            
            # Instantiate Architectural Components
            connector = MondayConnector(MONDAY_API_KEY)
            cleaner = DataResilience()
            bi_engine = BusinessIntelligence()
            llm = ResponseManager()
            
            # Fetch
            raw_deals = connector.fetch_board_data(DEALS_BOARD_ID)
            raw_wo = connector.fetch_board_data(WORK_ORDERS_BOARD_ID)
            
            if "error" in raw_deals:
                cleaner.data_quality_notes.append(f"MONDAY API ERROR (PIPELINE): {raw_deals['error']}")
            if "error" in raw_wo:
                cleaner.data_quality_notes.append(f"MONDAY API ERROR (OPERATIONS): {raw_wo['error']}")
                
            deals_extracted = connector.extract_items(raw_deals)
            wo_extracted = connector.extract_items(raw_wo)
            
            # Resilience & Normalize
            df_pipeline = cleaner.clean_deals(deals_extracted)
            df_operations = cleaner.clean_work_orders(wo_extracted)
            
            # BI Aggregation
            pipeline_health = bi_engine.calculate_pipeline_metrics(df_pipeline)
            sector_health = bi_engine.calculate_sector_health(df_pipeline)
            cross_board_risk = bi_engine.cross_board_operational_risk(df_pipeline, df_operations)
            deal_stage_funnel = bi_engine.calculate_deal_stage_funnel(df_pipeline)
            
            raw_cols = [c for c in df_pipeline.columns if c in ["Deal Name", "Sector_Norm", "Status_Norm", "Deal_Value", "Date_Norm"]]
            raw_data_csv = df_pipeline[raw_cols].to_csv(index=False) if not df_pipeline.empty else "No deals found."
            
            # Truncate raw data to prevent Groq API token limits (413 Payload Too Large)
            # 1 token is approx 4 chars. Limit to 5,000 chars (~1250 tokens) to be extremely safe on the 6000 TPM limit
            if len(raw_data_csv) > 5000:
                raw_data_csv = raw_data_csv[:5000] + "\n...[DATA TRUNCATED DUE TO LLM TOKEN LIMITS]..."
            
            context_snapshot = f"""
            [BI Engine Output]:
            Pipeline & Revenue Metrics: {pipeline_health}
            Sector Performance Breakdown: {sector_health}
            Cross-Board Capacity/Bottlenecks: {cross_board_risk}
            Deal Stages (Funnel): {deal_stage_funnel}
            
            [Raw Cleaned Deal Data for Dynamic Queries (e.g. answering "What is this week's revenue?")]:
            {raw_data_csv}
            """
            
            # LLM Interpretation
            history = st.session_state.messages[:-1]
            final_response = llm.run_agent(prompt, history, context_snapshot, cleaner.data_quality_notes)
            
            wants_visuals = []
            valid_tags = ["[CHART: SECTOR_PIE]", "[CHART: PIPELINE_BAR]", "[CHART: BOTTLENECK_BAR]", "[CHART: DEAL_STAGE_FUNNEL]"]
            for tag in valid_tags:
                if tag in final_response:
                    wants_visuals.append(tag)
                    final_response = final_response.replace(tag, "")
            
            final_response = final_response.strip()
            
            # -----------------------------------------------
            # Visualizations Implementation
            # -----------------------------------------------
            import plotly.express as px
            
            # Display LLM Response first
            st.markdown(final_response)
            st.session_state.messages.append({"role": "assistant", "content": final_response})
            
            if wants_visuals:
                st.divider()
                st.subheader("📊 Relevant Data Visualizations")
                
                # Render pie and bar side by side if both requested
                cols = st.columns(min(len(wants_visuals), 2))
                col_idx = 0
                
                for chart_type in wants_visuals:
                    current_col = cols[col_idx % len(cols)]
                    
                    with current_col:
                        if chart_type == "[CHART: SECTOR_PIE]":
                            st.markdown("**Revenue by Sector (Pipeline)**")
                            if sector_health:
                                sector_df = pd.DataFrame(list(sector_health.items()), columns=['Sector', 'Revenue'])
                                sector_df = sector_df[sector_df['Revenue'] > 0]
                                if not sector_df.empty:
                                    fig = px.pie(sector_df, values='Revenue', names='Sector', hole=0.4, 
                                                 color_discrete_sequence=px.colors.qualitative.Pastel)
                                    fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    st.info("No active revenue data found.")
                                    
                        elif chart_type == "[CHART: PIPELINE_BAR]":
                            st.markdown("**Pipeline Health Snapshot**")
                            if pipeline_health:
                                pipe_df = pd.DataFrame(list(pipeline_health.items()), columns=['Metric', 'Value'])
                                if not pipe_df.empty:
                                    fig = px.bar(pipe_df, x='Metric', y='Value', color='Metric', text_auto='.2s',
                                                  color_discrete_sequence=px.colors.diverging.Tealrose)
                                    fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="Value ($)", margin=dict(t=20, b=0, l=0, r=0))
                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    st.info("No pipeline metrics available.")
                                    
                        elif chart_type == "[CHART: BOTTLENECK_BAR]":
                            st.markdown("**Operations Bottleneck Risk**")
                            if isinstance(cross_board_risk, dict):
                                b_df = pd.DataFrame([
                                    {"Status": "Total Won Sales Tracked", "Count": cross_board_risk.get("Total Won Sales Tracked", 0)},
                                    {"Status": "Bottlenecks Identified", "Count": cross_board_risk.get("Bottlenecks Identified", 0)}
                                ])
                                fig = px.bar(b_df, x="Status", y="Count", color="Status", text_auto=True,
                                             color_discrete_map={"Total Won Sales Tracked": "green", "Bottlenecks Identified": "red"})
                                fig.update_layout(showlegend=False, xaxis_title="", margin=dict(t=20, b=0, l=0, r=0))
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("Could not calculate cross board risk.")
                                
                        elif chart_type == "[CHART: DEAL_STAGE_FUNNEL]":
                            st.markdown("**Deals by Stage**")
                            if deal_stage_funnel:
                                f_df = pd.DataFrame(list(deal_stage_funnel.items()), columns=['Stage', 'Count'])
                                f_df = f_df.sort_values(by="Stage")
                                fig = px.funnel(f_df, x='Count', y='Stage', color_discrete_sequence=px.colors.sequential.Teal)
                                fig.update_layout(margin=dict(t=20, b=0, l=0, r=0))
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("No deal stage data found.")
                    
                    col_idx += 1
