import os
import re
import time
from datetime import date, timedelta

# Third-party imports
import plotly.graph_objects as go
import duckdb
import pandas as pd
import requests
import webbrowser
from colorama import Fore

# LangChain imports
from langchain.agents import Tool, AgentExecutor, create_react_agent
from langchain.prompts import PromptTemplate
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler

# LLM-specific imports
from langchain_openai import OpenAI
from langchain_anthropic import ChatAnthropic
import google.generativeai as genai
from langchain_google_genai import ChatGoogleGenerativeAI
from google.generativeai.types import HarmCategory, HarmBlockThreshold

#>-----------------------------------------------------------
# Define a global variable to store the color sequence
color_sequence = [Fore.RED, Fore.GREEN, Fore.BLUE, Fore.YELLOW, Fore.MAGENTA, Fore.CYAN]
def analyze_stock_data(data):
    """Analyzes stock data and calculates various metrics.

    Args:
        data: A pandas DataFrame containing stock data with columns:
            - Date
            - Ticker
            - Open
            - High
            - Low
            - Close

    Returns:
        A dictionary containing the following calculated metrics:
            - daily_returns: Daily percentage returns
            - cumulative_returns: Cumulative percentage returns
            - volatility: Daily volatility
            - average_price: Average price
            - max_price: Maximum price and its date (in "YYYY-MM-DD" format)
            - min_price: Minimum price and its date (in "YYYY-MM-DD" format)
    """

    # Convert the 'Date' column to datetime format
    data['Date'] = pd.to_datetime(data['Date'])

    # Calculate daily percentage returns
    data['Daily Returns'] = (data['Close'] / data['Close'].shift(1) - 1) * 100

    # Calculate cumulative percentage returns
    data['Cumulative Returns'] = (data['Close'] / data['Close'][0] - 1) * 100

    # Calculate daily volatility
    volatility = data['Daily Returns'].std()

    # Calculate average price
    average_price = data['Close'].mean()

    # Calculate maximum and minimum prices with their dates
    max_price_index = data['Close'].idxmax()
    max_price = (data['Close'].max(), data.loc[max_price_index, 'Date'].strftime('%Y-%m-%d'))
    min_price_index = data['Close'].idxmin()
    min_price = (data['Close'].min(), data.loc[min_price_index, 'Date'].strftime('%Y-%m-%d'))

    # Create a dictionary to store the calculated metrics
    results = {
        'volatility': volatility,
        'average_price': average_price,
        'max_price': max_price,
        'min_price': min_price
    }

    # Print the extracted metrics
    print(Fore.WHITE + "Volatility:", volatility)
    print(Fore.WHITE + "Average Price:", average_price)
    print(Fore.WHITE + "Maximum Price:", max_price)
    print(Fore.WHITE + "Minimum Price:", min_price)

    return results

def delete_html_files(folder_path: str):
    """
    Delete all files with the .html extension in the specified folder.

    Parameters:
    - folder_path: str: The path to the folder from which to delete HTML files.
    """
    try:
        # List all files in the specified directory
        files_deleted = 0
        for filename in os.listdir(folder_path):
            # Check for .html files
            if filename.endswith('.html'):
                file_path = os.path.join(folder_path, filename)
                os.remove(file_path)
                files_deleted += 1
                print(f"Deleted: {file_path}")

        if files_deleted == 0:
            print("No .html files found in the directory.")
        else:
            print(f"Deleted {files_deleted} .html file(s).")

    except Exception as e:
        print(f"Error deleting files: {e}")
#<-----------------------------------------------------------

# Function to fetch stock data from the database
def parse_date_range(query_parts, is_default_range=False):
    today = date.today()

    if is_default_range or len(query_parts) <= 1 or not query_parts[1]:
        # Default date range logic
        start_date = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        end_date = today
    else:
        # Parse the date range from query_parts[1]
        date_range = query_parts[1]
        if " to " in date_range:
            start_date_str, end_date_str = date_range.split(" to ")
            start_date = date.fromisoformat(start_date_str)
            end_date = date.fromisoformat(end_date_str)
        else:
            # Only start date provided
            start_date = date.fromisoformat(date_range)
            end_date = today

    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

def fetch_stock_data(query: str) -> pd.DataFrame:
    db_path = r'C:\Users\riper\OneDrive\Documents\Python\Local\Stocks\financials_data.db'
    # Split the query by commas and strip whitespace
    print("\nFetch Stock Data input: "+query+"\n")
    query_parts = [part.strip() for part in query.split(',')]    
    # Parse the query string
    symbol_to_search = query_parts[0].upper() 

    # Determine date range   
    is_default_range = any('default date range' in part.lower() for part in query_parts)
    start_date, end_date = parse_date_range(query_parts, is_default_range)

    # Connect to the database and fetch data
    with duckdb.connect(db_path) as con:
        tables = con.execute("SHOW TABLES").fetchdf()['name'].tolist()
        parquet_table_name = next((t for t in tables if t.startswith("Stocks_")), None)
        if parquet_table_name:
            query = f"""
                SELECT Date, Ticker, Open, High, Low, Close FROM {parquet_table_name}
                WHERE Ticker = '{symbol_to_search}'
                AND Date BETWEEN '{start_date}' AND '{end_date}'
            """
            print(query)
            df = con.execute(query).fetchdf()

            # Ensure 'Date' is in datetime format
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values(by='Date') 
            #print("\n")
            #analyze_stock_data(df)
            #print("\n")
            return df

        else:
            return pd.DataFrame()  # Return an empty DataFrame if no table is found

def fetch_stocks1(query: str):
    df = fetch_stock_data(query)

    if df.empty:
        return "No data found for the given query."

    # Analyze the data and return the results
    return analyze_stock_data(df)

def fetch_stocks(query: str):
    df = fetch_stock_data(query)

    if df.empty:
        print("No data found for the given query.")
        return

    # Ensure 'Date' is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])

    # Extract symbol_to_search from the query (assuming it's the first word)
    symbol_to_search = query.split()[0].upper()
    symbol_to_search = symbol_to_search.replace(",", "")

    return df.to_markdown(index=False, numalign="left", stralign="left")

def plot_chart(query: str):
    df = fetch_stock_data(query)

    if df.empty:
        print("No data found for the given query.")
        return

    # Ensure 'Date' is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])

    # Extract symbol_to_search from the query (assuming it's the first word)
    symbol_to_search = query.split()[0].upper()
    symbol_to_search = symbol_to_search.replace(",", "")

    # Create a candlestick chart using Plotly
    fig = go.Figure(data=[go.Candlestick(x=df['Date'],
                                          open=df['Open'],
                                          high=df['High'],
                                          low=df['Low'],
                                          close=df['Close'],
                                          name='Candlesticks')])

    # Update layout settings
    fig.update_layout(title=f'Candlestick Chart for {symbol_to_search}',
                      xaxis_title='Date',
                      yaxis_title='Price',
                      xaxis_rangeslider_visible=False)

    # Save the figure as an HTML file
    html_file_path = f"candlestick_chart_{symbol_to_search}.html"
    fig.write_html(html_file_path)

    print(f"Candlestick chart saved as {html_file_path}")

    # Automatically open the HTML file in the default web browser
    webbrowser.open('file://' + os.path.realpath(html_file_path))
    return df.to_markdown(index=False, numalign="left", stralign="left")

def fetch_stock_news(ticker):
    """
    Fetch the latest financial news articles related to the given stock ticker symbol in English,
    generate an HTML file, and open it in the default browser.

    Parameters:
    ticker (str): The stock ticker symbol for which to fetch the news.
    """

    api_key = ''  # Replace with your actual API key
    query = f"{ticker} financial OR finance OR earnings OR stock"
    url = f'https://newsapi.org/v2/everything?q={query}&language=en&apiKey={api_key}'

    try:
        response = requests.get(url)
        response.raise_for_status()

        news_data = response.json()
        articles = news_data.get('articles', [])

        html_content = f"""
        <html>
        <head>
            <title>Financial News for {ticker}, by SmartBob</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .article {{ border: 1px solid #ccc; padding: 10px; margin-bottom: 10px; }}
            </style>
        </head>
        <body>
            <h1>Financial News for {ticker}, by SmartBob</h1>
        """

        if not articles:
            html_content += f"<p>No news articles found for ticker: {ticker}</p>"
        else:
            for article in articles:
                html_content += f"""
                    <div class="article">
                        <h3>{article['title']}</h3>
                        <p>{article.get('description', 'No description available.')}</p>
                        <p>Published At: {article['publishedAt']}</p>
                        <p><a href="{article['url']}" target="_blank">Read More</a></p>
                    </div>
                """

        html_content += """
        </body>
        </html>
        """

        # Write HTML content to a file
        file_path = f"{ticker}_news.html"
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        # Open the HTML file in the default browser
        webbrowser.open('file://' + os.path.realpath(file_path))

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def get_llm(llm_choice):
    if llm_choice == 'openai':
        api_key = load_api_key('openai_api_key.txt')
        return OpenAI(temperature=0.0, openai_api_key=api_key)
    elif llm_choice == 'claude':
        api_key = load_api_key('claude_api_key.txt')
        return ChatAnthropic(
            model="claude-3-sonnet-20240229",
            anthropic_api_key=api_key,
            streaming=True,
            callbacks=[StreamingStdOutCallbackHandler()]
        )
    elif llm_choice == 'gemini':
        api_key = load_api_key('gemini_api_key.txt')
        genai.configure(api_key=api_key)
        return ChatGoogleGenerativeAI(
            model="gemini-pro",
            google_api_key=api_key,
            temperature=0.3,
            top_p=1,
            top_k=1,
            max_output_tokens=2048,
            safety_settings={
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            },
            streaming=True,
            callbacks=[StreamingStdOutCallbackHandler()],
        )
    else:
        raise ValueError("Invalid LLM choice. Please choose 'openai', 'claude', or 'gemini'.")

def load_api_key(filename):
    key_file_path = os.path.join(os.path.dirname(__file__), filename)
    try:
        with open(key_file_path, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"API key file '{filename}' not found.")

def create_agent(llm):
    tools = [
        Tool(
            name="Plot Chart",
            func=plot_chart,
            description="Plot Candlestick Chart and show stock price for a given stock symbol and date range. Use format: 'symbol, YYYY-MM-DD to YYYY-MM-DD'"
        ),
#        Tool(
#            name="Fetch News",
#            func=fetch_stock_news,
#            description="Get the latest news related to a given stock symbol."
#        ),
        Tool(
            name="Get Data",
            func=fetch_stocks,
            description="Get historical stock data for a given stock symbol and date range. Use format: 'symbol, YYYY-MM-DD to YYYY-MM-DD'"
        ),
    ]

    prompt = PromptTemplate.from_template(
        """You are an AI assistant that helps with stock analysis and visualization. 
        Answer the following question: {input}

        You have access to the following tools:
        {tools}

        IMPORTANT: You MUST use one of the available tools to answer the question. Do not try to answer without using a tool.

        Use the following format:

        Question: the input question you must answer
        Thought: you should always think about which tool to use
        Action: the action to take, should be one of [{tool_names}]
        Action Input: the input to the action, including the stock symbol and date range if provided (use format YYYY-MM-DD for dates)
        Observation: the result of the action
        Final Answer: a concise summary of the results or the answer to the original question

        Remember, precision in financial data analysis is crucial. Always use the date range format YYYY-MM-DD to YYYY-MM-DD when working with dates.
        If no date range is specified, use the last month as the default range.
        
        {agent_scratchpad}
        """
    )

    agent = create_react_agent(llm, tools, prompt)
    return AgentExecutor(
        agent=agent, 
        tools=tools, 
        verbose=True, 
        handle_parsing_errors=True,
        max_iterations=1
    )

def process_user_input(user_input, current_llm, agent_executor):
    # Handle the case of not informing any dates
    if not any(date in user_input for date in ['to', '-']):
        today = date.today()
        first_day_of_previous_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        user_input += f", {first_day_of_previous_month} to {today}"

    try:
        start_time = time.time()
        response = agent_executor.invoke({"input": user_input})
        end_time = time.time()

        # Filter out unwanted text from the response
        response['output'] = re.sub(r'[\n\s]*', '', response['output'])

        print(f"\nChatBot ({current_llm.capitalize()}): {response['output']}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
    except Exception as e:
        print(f"ChatBot: An error occurred: {e}. Please try rephrasing your request or providing more information.")


def main():
    print("Welcome to the Unified LLM Chatbot - By Ricardo!")

    llm_map = {'1': 'openai', '2': 'claude', '3': 'gemini'}
    current_llm = None
    agent_executor = None

    while True:
        if not current_llm:
            choice = input("\nChoose your LLM for this interaction \n1. OpenAI\n2. Claude (Anthropic)\n3. Gemini (Google) or 'bye bob' to exit: ")
        else:
            print(f"\nCurrent LLM: {current_llm.capitalize()} - Change LLM to change it, or - bye bob")
            user_input = input(f"You ({current_llm.capitalize()}): ")

            if user_input.lower() == 'bye bob':
                print("ChatBot: Bye! Have a great day!")
                break

            if user_input.lower() == 'change llm':
                current_llm = None
                agent_executor = None
                continue
            else:
                process_user_input(user_input, current_llm, agent_executor)
                continue

        if choice.lower() == 'bye bob':
            print("ChatBot: Bye! Have a great day!")
            break

        llm_choice = llm_map.get(choice)

        if not llm_choice:
            print("Invalid choice. Please try again.")
            continue

        try:
            llm = get_llm(llm_choice)
            agent_executor = create_agent(llm)
            current_llm = llm_choice
            print(f"Switched to {current_llm.capitalize()} LLM.")
        except Exception as e:
            print(f"Error initializing LLM: {e}")
            continue


if __name__ == "__main__":
    main()
    delete_html_files(os.path.dirname(__file__))