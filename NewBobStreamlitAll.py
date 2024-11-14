import os
import duckdb
import google.generativeai as genai
from anthropic import Anthropic
from openai import OpenAI
import pandas as pd
import streamlit as st
from typing import Optional, Protocol
from abc import ABC, abstractmethod
import asyncio
import nest_asyncio
from concurrent.futures import ThreadPoolExecutor
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

class LLMProvider(ABC):
    """Abstract base class for LLM providers"""
    @abstractmethod
    def initialize(self, api_key: str):
        """Initialize the LLM with API key"""
        pass

    @abstractmethod
    def generate_response(self, prompt: str) -> str:
        """Generate response from the LLM"""
        pass

class GeminiProvider(LLMProvider):
    def initialize(self, api_key: str):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-pro')

    def generate_response(self, prompt: str) -> str:
        response = self.model.generate_content(prompt)
        return response.text

class ClaudeProvider(LLMProvider):
    def initialize(self, api_key: str):
        self.client = Anthropic(api_key=api_key)

    def generate_response(self, prompt: str) -> str:
        response = self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": prompt
            }]
        )
        return response.content[0].text

class OpenAIProvider(LLMProvider):
    def initialize(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

    def generate_response(self, prompt: str) -> str:
        response = self.client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[{
                "role": "user",
                "content": prompt
            }],
            max_tokens=2000,
            temperature=0.7
        )
        return response.choices[0].message.content

class FinancialDatabaseChatbot:
    def __init__(self):
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.schema = {}
        self._executor = ThreadPoolExecutor(max_workers=1)
        self.llm_provider = None
        self.current_model = None

    def set_llm_provider(self, provider_name: str, api_key: str):
        providers = {
            'gemini': GeminiProvider,
            'claude': ClaudeProvider,
            'openai': OpenAIProvider
        }
        
        if provider_name not in providers:
            raise ValueError(f"Unsupported LLM provider: {provider_name}")
        
        provider_class = providers[provider_name]
        self.llm_provider = provider_class()
        self.llm_provider.initialize(api_key)
        
        model_names = {
            'gemini': 'Gemini 1.5 Pro',
            'claude': 'Claude 3 Sonnet',
            'openai': 'GPT-4 Turbo'
        }
        self.current_model = model_names.get(provider_name, provider_name)

    def get_table_schema(self):
        if not self.conn:
            return {}

        schema = {}
        try:
            target_tables = ['Stocks_Float', 'Crypto_Float', 'Stocks_Info']
            
            for table in target_tables:
                try:
                    result = self.conn.execute(f"PRAGMA table_info({table});").fetchall()
                    columns = [{"name": row[1], "type": row[2]} for row in result]
                    schema[table] = columns
                except Exception as e:
                    st.warning(f"Table '{table}' not found or error accessing it: {str(e)}")

            return schema
        except Exception as e:
            st.error(f"Error fetching schema: {str(e)}")
            return {}

    def connect_to_database(self, db_path: str) -> bool:
        try:
            self.conn = duckdb.connect(db_path, read_only=True)
            self.schema = self.get_table_schema()
            return True if self.schema else False
        except Exception as e:
            st.error(f"Error connecting to database: {str(e)}")
            return False

    def generate_schema_context(self) -> str:
        if not self.schema:
            return "No schema information available."

        context = "This is a financial database with the following structure:\n\nTables and their columns:\n"

        for table_name, columns in self.schema.items():
            context += f"\nTable: {table_name}\nColumns:\n"
            for col in columns:
                context += f"- {col['name']} ({col['type']})\n"

        context += "\nPotential Key Relationships:\n"
        common_columns = self.identify_common_columns()
        for col_name, tables in common_columns.items():
            if len(tables) > 1:
                context += f"- Column '{col_name}' appears in tables: {', '.join(tables)}\n"

        return context

    def identify_common_columns(self):
        common_columns = {}
        for table, columns in self.schema.items():
            for col in columns:
                col_name = col['name'].lower()
                if col_name not in common_columns:
                    common_columns[col_name] = []
                common_columns[col_name].append(table)
        return {k: v for k, v in common_columns.items() if len(v) > 1}

    def ask_question(self, question: str) -> dict:
        if not self.conn:
            return {"error": "Please connect to a database first."}
        
        if not self.llm_provider:
            return {"error": "Please select an LLM provider first."}

        schema_context = self.generate_schema_context()
        
        prompt = f"""You are a financial database expert assistant. Below is the schema of a financial database:

            {schema_context}

            Based on this schema, please answer the following question: {question}

            Generate a response in the following format:
            1. First provide an explanation of how to solve the question
            2. Then provide the SQL query
            3. Finally, end with exactly "EXECUTE_QUERY:" followed by just the SQL query on the next line

            Important guidelines:
            - Do not use markdown code blocks or backticks in your response
            - ALWAYS use UPPERCASE for stock tickers (e.g., 'AAPL' not 'aapl')
            - For crypto tickers, ALWAYS use the format 'XXX-USD' (e.g., 'BTC-USD', 'ETH-USD')
            - For candlestick charts, always include Date, Open, High, Low, and Close columns in that order
            - When showing price movements over time, order by Date ASC to ensure proper chart rendering
            - Query should be written in plain text

            Use DuckDB SQL syntax and note:
            - Use CAST(Date AS DATE) when comparing dates
            - For relative dates, use CURRENT_DATE - INTERVAL '30 days' format
            - For specific dates, use DATE '2024-01-01' format
            - String operations use || for concatenation
            - Stock tickers in the database are stored in UPPERCASE
            - Crypto tickers are stored with '-USD' suffix"""

        try:
            response_text = self.llm_provider.generate_response(prompt)
            
            if "EXECUTE_QUERY:" in response_text:
                parts = response_text.split("EXECUTE_QUERY:")
                explanation = parts[0].strip()
                query = parts[1].strip()
                query = query.replace('```sql', '').replace('```', '').strip()
                
                try:
                    result = self.conn.execute(query).fetchdf()
                    return {
                        "explanation": explanation,
                        "query": query,
                        "result": result,
                        "model": self.current_model
                    }
                except Exception as e:
                    return {
                        "explanation": explanation,
                        "query": query,
                        "error": str(e),
                        "model": self.current_model
                    }
            
            return {
                "explanation": response_text,
                "model": self.current_model
            }
                
        except Exception as e:
            return {
                "error": f"Error generating response: {str(e)}",
                "model": self.current_model
            }

def load_api_key(filename):
    key_file_path = os.path.join(os.path.dirname(__file__), filename)
    try:
        with open(key_file_path, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"API key file '{filename}' not found.")

def initialize_session_state():
    if "chatbot" not in st.session_state:
        st.session_state.chatbot = None
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "current_llm" not in st.session_state:
        st.session_state.current_llm = "gemini"

def create_candlestick_chart(df, title="Stock Price"):
    fig = go.Figure(data=[go.Candlestick(x=df['Date'],
                                        open=df['Open'],
                                        high=df['High'],
                                        low=df['Low'],
                                        close=df['Close'])])
    
    fig.update_layout(
        title=title,
        yaxis_title='Price',
        xaxis_title='Date',
        template='plotly_dark',
        xaxis_rangeslider_visible=False
    )
    
    return fig

def format_chat_message(message: dict, is_user: bool = False):
    if is_user:
        st.write(f'🧑‍💼 **You:** {message["text"]}')
    else:
        model_name = message.get("model", "Unknown Model")
        st.write(
            f'🤖 **Assistant** <span style="background-color: #2E303E; color: #FFFFFF; padding: 2px 6px; border-radius: 4px; font-size: 0.8em; margin-left: 8px;">powered by {model_name}</span>',
            unsafe_allow_html=True
        )
        
        if "explanation" in message:
            st.write(message["explanation"])
        if "query" in message:
            st.code(message["query"], language="sql")
        if "result" in message and not message["result"].empty:
            if all(col in message["result"].columns for col in ['Date', 'Open', 'High', 'Low', 'Close']):
                st.plotly_chart(
                    create_candlestick_chart(
                        message["result"], 
                        title=f"Price Chart for {message['result']['Ticker'].iloc[0] if 'Ticker' in message['result'].columns else 'Stock'}"
                    ),
                    use_container_width=True
                )
            st.dataframe(message["result"], use_container_width=True)
        if "error" in message:
            st.error(message["error"])

def get_example_questions():
    return {
        "Stock Performance Analysis": [
            "Bring me each crypto and its highest close value for 2024 and the date of said value, use CTE, order by the highest value of the last result",
            "Bring me from crypto BTC-USD its highest close value by month for 2024 and the date of said value, use CTE",
            "Show me AAPL's candlestick chart for the last 30 days",
            "What was the highest trading volume day for AAPL in 2024?",
            "Show me the top 5 highest closing prices for MSFT in the past month",
            "Compare the average closing prices of AAPL, MSFT, and GOOGL in 2024",
            "Which stock had the highest percentage gain in January 2024?"
        ],
        "Trading Volume Insights": [
            "Which days had unusual trading volume for TSLA in 2024?",
            "Show me the top 10 highest volume trading days across all stocks in 2024",
            "What's the average daily trading volume for NVDA compared to AMD?"
        ],
        "Price Movement Analysis": [
            "Show me MSFT's daily price movements with candlesticks for February 2024",
            "Display the candlestick chart for TSLA's most volatile week in 2024",
            "Which stock had the most volatile price movements in 2024?",
            "Show me days where AAPL had more than 5% price change",
            "What's the biggest single-day price drop for META in 2024?"
        ],
        "Crypto Analysis": [
            "Show me BTC-USD's candlestick chart for the last week",
            "Compare BTC-USD and ETH-USD price movements in 2024",
            "What's the highest price BTC-USD reached in 2024?",
            "Show me the correlation between BTC-USD and ETH-USD prices",
            "What's the average daily trading volume for BTC-USD in 2024?",
            "Show me days where ETH-USD had more than 10% price change"
        ],
        "Market Trends": [
            "Which tech stocks have shown consistent growth in 2024?",
            "What's the average daily trading volume across all tech stocks?",
            "Show me stocks that are trading above their 30-day average price"
        ]
    }

def main():
    st.set_page_config(
        page_title="Financial Database Assistant",
        page_icon="💰",
        layout="wide"
    )

    # Add custom CSS for sticky positioning and layout
    st.markdown("""
        <style>
        .fixed-top {
            position: sticky;
            top: 0;
            z-index: 999;
            background-color: #0E1117;
            padding: 1rem 0;
            border-bottom: 1px solid #1E1E1E;
        }
        .main-content {
            margin-top: 1rem;
        }
        .stButton button {
            width: 100%;
        }
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track {
            background: #1E1E1E;
        }
        ::-webkit-scrollbar-thumb {
            background: #3E3E3E;
            border-radius: 4px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #555;
        }
        .stCodeBlock {
            max-height: 300px;
            overflow-y: auto;
        }
        .stChatInput {
            position: static !important;
            margin-top: 1rem;
            margin-bottom: 1rem;
        }
        .stSelectbox {
            min-width: 200px;
        }
        div[data-testid="stChatInput"] {
            position: static !important;
            margin-top: 1rem;
        }
        </style>
    """, unsafe_allow_html=True)

    # Initialize session state and load configuration
    initialize_session_state()
    gemini_api_key = load_api_key('gemini_api_key.txt')
    claude_api_key = load_api_key('claude_api_key.txt')
    openai_api_key = load_api_key('openai_api_key.txt')
    db_path = "c:/Users/riper/OneDrive/Documents/Python/Local/Stocks/financials_data.db"

    # Create tabs
    tab1, tab2 = st.tabs(["Chat Assistant", "Analysis Dashboard"])

    with tab1:
        # Sidebar for LLM selection
        with st.sidebar:
            st.title("Settings")
            selected_llm = st.radio(
                "Select LLM Provider",
                options=["gemini", "claude", "openai"],
                key="llm_selector"
            )

            # Initialize or update chatbot if LLM changed
            if st.session_state.chatbot is None or selected_llm != st.session_state.current_llm:
                st.session_state.chatbot = FinancialDatabaseChatbot()
                api_key = {
                    "gemini": gemini_api_key,
                    "claude": claude_api_key,
                    "openai": openai_api_key
                }[selected_llm]
                
                st.session_state.chatbot.set_llm_provider(selected_llm, api_key)
                st.session_state.current_llm = selected_llm
                
                if st.session_state.chatbot.connect_to_database(db_path):
                    st.success(f"Connected to database using {selected_llm.title()}")
                    st.markdown("### Database Schema")
                    st.code(st.session_state.chatbot.generate_schema_context())
                else:
                    st.error("Failed to connect to database.")

        # Questions and Input Section (Fixed at top)
        questions_container = st.container()
        with questions_container:
            st.markdown('<div class="fixed-top">', unsafe_allow_html=True)
            
            # Title
            st.title("💰 Financial Database Assistant")
            
            # Example Questions Section
            st.markdown("### 📝 Example Questions")
            
            # Create two columns for category and question selection
            col1, col2 = st.columns([1, 3])
            
            with col1:
                selected_category = st.selectbox(
                    "Select Category",
                    options=list(get_example_questions().keys()),
                    key="category_selector"
                )
            
            with col2:
                selected_question = st.selectbox(
                    "Select Question",
                    options=get_example_questions()[selected_category],
                    key="question_selector"
                )

            # Action Buttons and Chat Input
            button_col1, button_col2, button_col3 = st.columns([1, 1, 2])
            with button_col1:
                execute_button = st.button("Execute Question", type="primary", use_container_width=True)
            with button_col2:
                clear_history = st.button("Clear History", type="secondary", use_container_width=True)
                if clear_history:
                    st.session_state.chat_history = []
                    st.rerun()

            # Chat input in the fixed section
            if st.session_state.chatbot:
                user_input = st.chat_input("Ask a question about your financial data...")
            
            st.markdown('</div>', unsafe_allow_html=True)

        # Chat History Section (Scrollable)
        chat_container = st.container()
        with chat_container:
            st.markdown('<div class="main-content">', unsafe_allow_html=True)
            
            # Only show Chat History if there are messages
            if st.session_state.chat_history:
                st.markdown("### 💬 Chat History")
                for message in st.session_state.chat_history:
                    format_chat_message(message, message.get("is_user", False))

            # Process user input or executed example question
            if st.session_state.chatbot:
                if user_input or execute_button:
                    if user_input:
                        question = user_input
                    elif execute_button:
                        question = selected_question
                    
                    # Add user message to chat history
                    st.session_state.chat_history.append({"text": question, "is_user": True})
                    
                    # Get and display assistant response
                    response = st.session_state.chatbot.ask_question(question)
                    st.session_state.chat_history.append(response)
                    
                    # Rerun to update the chat display
                    st.rerun()
            else:
                st.info("Unable to initialize the chatbot. Please check your API keys and database path.")
            
            st.markdown('</div>', unsafe_allow_html=True)

    with tab2:
        st.title("Stock & Crypto Analysis Dashboard")
        
        queries = {
            "Bitcoin Monthly High/Low Analysis": """
            WITH RankedRows AS (
                SELECT 
                    "Date", "Ticker", "Close",
                    ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('MONTH', "Date") 
                        ORDER BY "Close" DESC) as RowNumHigh,
                    ROW_NUMBER() OVER(PARTITION BY DATE_TRUNC('MONTH', "Date") 
                        ORDER BY "Close" ASC) as RowNumLow
                FROM Crypto_Float
                WHERE "Ticker" = 'BTC-USD' AND "Date" >= DATE '2020-01-01'
            )
            SELECT * FROM RankedRows 
            WHERE RowNumHigh = 1 OR RowNumLow = 1
            ORDER BY "Date" DESC
            LIMIT 10
            """,
            "Stock Sector Distribution": """
            SELECT DISTINCT sector, COUNT(*) as count 
            FROM Stocks_Info 
            GROUP BY sector 
            ORDER BY count DESC
            """,
            "Latest Stock Prices": """
            SELECT "Date", "Ticker", "Close"
            FROM Stocks_Float
            WHERE "Date" >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY "Date" DESC, "Ticker"
            LIMIT 20
            """,
            "Crypto Performance Analysis": """
            SELECT 
                "Ticker",
                MIN("Close") as min_price,
                MAX("Close") as max_price,
                AVG("Close") as avg_price,
                COUNT(*) as days_traded
            FROM Crypto_Float
            GROUP BY "Ticker"
            ORDER BY avg_price DESC
            LIMIT 15
            """
        }
        
        query_option = st.selectbox(
            "Select Analysis",
            ["Custom Query"] + list(queries.keys())
        )
        
        if query_option == "Custom Query":
            query = st.text_area("Enter SQL query:", height=200)
        else:
            query = queries[query_option]
            st.code(query, language="sql")
            
        if st.button("Run Analysis"):
            if query:
                try:
                    results = run_query(query)
                    st.dataframe(results)
                    if not results.empty:
                        csv = results.to_csv(index=False)
                        st.download_button(
                            "Download Results",
                            csv,
                            "analysis_results.csv",
                            "text/csv",
                            key='download-csv'
                        )
                except Exception as e:
                    st.error(f"Error executing query: {str(e)}")

def run_query(query):
    """Execute a query and return results as a DataFrame."""
    db_path = r'C:\Users\riper\OneDrive\Documents\Python\Local\Stocks\financials_data.db'
    conn = duckdb.connect(database=db_path, read_only=True)
    try:
        return conn.execute(query).fetchdf()
    finally:
        conn.close()

if __name__ == "__main__":
    main()