# Standard library imports
import os  # Provides a way to interact with the operating system (e.g., read environment variables)

import plotly.graph_objects as go

# Third-party library imports
import duckdb  # A high-performance analytical database system
from datetime import date, timedelta  # For working with dates and time intervals
import pandas as pd  # Data manipulation and analysis library

# LangChain imports for creating AI agents and tools
from langchain.agents import Tool  # Defines tools that can be used by the AI agent
from langchain_openai import OpenAI  # OpenAI language model integration
from langchain.agents import create_react_agent  # Function to create a ReAct agent
from langchain.agents import AgentExecutor  # Executes the agent's actions
from langchain_core.prompts import PromptTemplate  # For creating customizable prompts
import requests
import webbrowser

#>-----------------------------------------------------------
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
def fetch_stock_data(query: str) -> pd.DataFrame:
    db_path = r'C:\Users\riper\OneDrive\Documents\Python\Local\Stocks\financials_data.db'
    # Split the query by commas and strip whitespace
    query_parts = [part.strip() for part in query.split(',')]    
    # Parse the query string
    symbol_to_search = query_parts[0].upper() 

    # Determine date range   
    # Extract dates (assuming they are always in the 2nd and 3rd positions if present)
    is_default_range = any('default date range' in part.lower() for part in query_parts)
    start_date = (date.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m-%d') if is_default_range or len(query_parts) <= 1 or not query_parts[1] else query_parts[1]
    end_date = date.today().strftime('%Y-%m-%d') if is_default_range or len(query_parts) <= 2 or not query_parts[2] else query_parts[2]

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

            df = con.execute(query).fetchdf()

            # Ensure 'Date' is in datetime format
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values(by='Date') 
            print("\n")
            return df

        else:
            return pd.DataFrame()  # Return an empty DataFrame if no table is found

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

#api_key = '511215ecfc53439c8a46f9550672c78e'
def fetch_stock_news(ticker):
    """
    Fetch the latest financial news articles related to the given stock ticker symbol in English,
    generate an HTML file, and open it in the default browser.

    Parameters:
    ticker (str): The stock ticker symbol for which to fetch the news.
    """

    api_key = '511215ecfc53439c8a46f9550672c78e'  # Replace with your actual API key
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

def main():
    """
    Reads the OpenAI API key from a file named 'openai_api_key.txt' located in the same directory as the script.
    Sets the OPENAI_API_KEY environment variable if the key is found.
    """

    key_file_path = '.\Stocks\openai_api_key.txt'  # Adjust if your file has a different name or location

    try:
        with open(key_file_path, 'r') as f:
            api_key = f.read().strip()  # Read the key and remove any leading/trailing whitespace

        os.environ["OPENAI_API_KEY"] = api_key
        print("OpenAI API key successfully loaded from file.")

    except FileNotFoundError:
        print(f"Error: File '{key_file_path}' not found. Please ensure the file exists and contains your API key.")
    except Exception as e:
        print(f"An error occurred while reading the API key: {e}")

    if not api_key:
        raise ValueError("Please set the OPENAI_API_KEY environment variable.")

    llm = OpenAI(temperature=0.0, openai_api_key=api_key)

    print("ChatBob: Hi there! What can I help you with today? (Type 'bye bob' to exit)")

    tools = [
        Tool(
            name="Plot Chart",
            func=plot_chart,
            description="Plot Candlestick Chart and show stock price."
        ),
        Tool(
            name="Fetch Stock News",
            func=fetch_stock_news,
            description="Get the latest news related to stock."
        ),
        Tool(
            name="Get Stocks",
            func=fetch_stocks,
            description="Get the available data and process it accordingly."
        ),
    ]

    # Create a prompt template

    prompt1 = PromptTemplate.from_template(
        """You are an AI assistant that helps with stock analysis and visualization. 
        Answer the following question: {input}

        You have access to the following tools:
        {tools}

        Use only one most fitting tool per request.
        Use the following format:

        Question: the input question you must answer
        Thought: you should always think about what to do
        Action: the action to take, should be one of [{tool_names}]
        Action Input: the input to the action,
        including the stock symbol and date range if provided
        Observation: the result of the action
        ... (this Thought/Action/Action Input/Observation can repeat N times)
        Thought: I now know the final answer
        Final Answer: the final answer to the original input question

        IMPORTANT: When analyzing stock data, you MUST accurately identify the highest closing price and its corresponding date. Follow these steps:

        1. Carefully examine the 'Close' column in the provided data.
        2. Find the maximum value in the 'Close' column. This is the highest closing price.
        3. Locate the exact date corresponding to this highest closing price.
        4. Double-check your findings to ensure accuracy.
        5. Always include the highest closing price and its precise date in your final answer.

        Remember, precision in financial data analysis is crucial. 
        
        {agent_scratchpad}
        """
    )

    prompt2 = PromptTemplate.from_template(
        "You are an AI assistant that helps with stock analysis and visualization. "
        "Answer the following question: \n\n{input}\n\n"
        "You have access to the following tools:\n"
        "\n\n{tools}\n\n"
        "Use only one most fitting tool per request."
        "Use the following format:\n"
        "Question: the input question you must answer\n"
        "Thought: you should always think about what to do\n"
        "Action: the action to take, should be one of \n\n{tool_names}\n\n"
        "Action Input: the input to the action, including the stock symbol and date range if provided\n"
        "Observation: the result of the action\n"
        "... (this Thought/Action/Action Input/Observation can repeat N times)\n"
        "Thought: I now know the final answer\n"
        "Final Answer: the final answer to the original input question\n"

        "IMPORTANT: When analyzing stock data, you MUST accurately identify the highest closing price and its corresponding date. Follow these steps:\n"

        "1. Carefully examine the 'Close' column in the provided data.\n"
        "2. Find the maximum value in the 'Close' column. This is the highest closing price.\n"
        "3. Locate the exact date corresponding to this highest closing price.\n"
        "4. Double-check your findings to ensure accuracy.\n"
        "5. Always include the highest closing price and its precise date in your final answer.\n"

        "Remember, precision in financial data analysis is crucial. \n"

        "\n\n{agent_scratchpad}\n\n"
    )

    prompt3 = PromptTemplate.from_template(
    """You are an AI assistant that helps with stock analysis and visualization. 
    Answer the following question: {input}

    You have access to the following tools:
    {tools}

    Use the following format:

    Question: the input question you must answer
    Thought: you should always think about what to do
    Action: the action to take, should be one of [{tool_names}]
    Action Input: the input to the action, including the stock symbol and date range if provided
    Observation: the result of the action
    Thought: analyze the observation and consider if more actions are needed 
    ... (this Thought/Action/Action Input/Observation can repeat N times)
    Thought: I now know the final answer
    Final Answer: the final answer to the original input question, including:
    1. A summary of the data analysis
    2. The highest closing price and its corresponding date
    3. Any other relevant information from the analysis

    Always analyze the data returned by the Plot Chart tool before giving a final answer.
    Ensure your final answer includes your reasoning and how you arrived at the conclusion.

    {agent_scratchpad}
    """
    )

    # Create the agent
    agent = create_react_agent(llm, tools, prompt2)

    # Create an agent executor
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, handle_parsing_errors=True)

    while True:
        user_input = input("You: ")
        if user_input.lower() == "bye bob":
            print("ChatBob: Bye! Have a great day!")
            break
        try:
            # Use the agent executor to process the user input
            response = agent_executor.invoke({"input": user_input})
            print(f"ChatBob: {response['output']}")
        except Exception as e:
            print(f"ChatBob: An error occurred: {e}. Please try rephrasing your request or providing more information.")

if __name__ == "__main__":
    main()
    delete_html_files('C:\\Users\\riper\\OneDrive\\Documents\\Python') 