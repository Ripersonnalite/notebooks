import streamlit as st
import google.generativeai as genai
from anthropic import Anthropic
from openai import OpenAI
import os
import time
from datetime import datetime

# Styling
st.set_page_config(page_title="AI Model Comparison", page_icon="🤖", layout="wide")

# Custom CSS for better UI with vertical layout
st.markdown("""
    <style>
    .assistant-header {
        background: linear-gradient(90deg, #2C2C44 0%, #1E1E2E 100%);
        padding: 12px 20px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .assistant-label {
        font-size: 1.2em;
        font-weight: bold;
        color: white;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    .model-badge {
        background-color: #4A4A6A;
        color: white;
        padding: 6px 12px;
        border-radius: 16px;
        font-size: 0.9em;
    }
    .response-content {
        background: #1E1E1E;
        padding: 20px;
        border-radius: 8px;
        margin-top: 10px;
    }
    .stTextArea textarea {
        height: 150px;
    }
    .chat-message {
        padding: 15px;
        margin: 10px 0;
        border-radius: 8px;
    }
    .user-message {
        background-color: #2C2C44;
    }
    .responses-container {
        display: flex;
        gap: 20px;
        margin-top: 20px;
    }
    .response-column {
        flex: 1;
        min-width: 0;
    }
    </style>
    """, unsafe_allow_html=True)

class ModelProvider:
    """Base class for model providers"""
    def __init__(self, name):
        self.name = name
        self.model = None
    
    def initialize(self, api_key):
        raise NotImplementedError
    
    def generate_response(self, prompt):
        raise NotImplementedError

class GeminiProvider(ModelProvider):
    def __init__(self):
        super().__init__("Gemini")
    
    def initialize(self, api_key):
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('gemini-1.5-pro')
    
    def generate_response(self, prompt):
        response = self.model.generate_content(prompt)
        return response.text

class ClaudeProvider(ModelProvider):
    def __init__(self):
        super().__init__("Claude")
    
    def initialize(self, api_key):
        self.client = Anthropic(api_key=api_key)
    
    def generate_response(self, prompt):
        response = self.client.messages.create(
            model="claude-3-sonnet-20240229",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text

class OpenAIProvider(ModelProvider):
    def __init__(self):
        super().__init__("GPT-4")
    
    def initialize(self, api_key):
        self.client = OpenAI(api_key=api_key)
    
    def generate_response(self, prompt):
        response = self.client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1000
        )
        return response.choices[0].message.content

def load_api_key(filename):
    """Load API key from file"""
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def format_message(message, model_name=None):
    """Format chat messages with custom styling"""
    if message["role"] == "user":
        return f"""
            <div class="chat-message user-message">
                <div class="assistant-label">👤 User</div>
                <div class="response-content">{message["content"]}</div>
            </div>
        """
    else:
        return f"""
            <div class="chat-message">
                <div class="assistant-header">
                    <div class="assistant-label">
                        🤖 Assistant
                        <span class="model-badge">powered by {model_name}</span>
                    </div>
                </div>
                <div class="response-content">{message["content"]}</div>
            </div>
        """

def display_conversation_turn(user_message, responses):
    """Display a single conversation turn with user message and model responses"""
    st.markdown(format_message(user_message), unsafe_allow_html=True)
    
    # Create columns for each model response
    cols = st.columns(len(responses))
    
    # Display responses in vertical columns
    for col, response in zip(cols, responses):
        with col:
            st.markdown(format_message(response, response.get("model")), unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "model_providers" not in st.session_state:
        st.session_state.model_providers = {}

def main():
    st.title("🤖 AI Model Comparison by Ricardo")
    
    initialize_session_state()
    
    # Load API keys
    api_keys = {
        "gemini": load_api_key("gemini_api_key.txt"),
        "claude": load_api_key("claude_api_key.txt"),
        "openai": load_api_key("openai_api_key.txt")
    }
    
    # Initialize providers if not already done
    if not st.session_state.model_providers:
        providers = {
            "gemini": GeminiProvider(),
            "claude": ClaudeProvider(),
            "openai": OpenAIProvider()
        }
        
        for name, provider in providers.items():
            if api_keys[name]:
                try:
                    provider.initialize(api_keys[name])
                    st.session_state.model_providers[name] = provider
                except Exception as e:
                    st.error(f"Error initializing {name}: {str(e)}")
    
    # Model selection
    selected_models = st.multiselect(
        "Select models to compare",
        options=list(st.session_state.model_providers.keys()),
        default=list(st.session_state.model_providers.keys())
    )
    
    # User input
    user_input = st.text_area("Enter your message:", placeholder="Type your message here...")
    
    # Control buttons
    col1, col2 = st.columns([1, 5])
    with col1:
        send_button = st.button("Send", type="primary", use_container_width=True)
    with col2:
        clear_button = st.button("Clear History", type="secondary")
    
    if clear_button:
        st.session_state.chat_history = []
        st.rerun()
    
    if send_button and user_input:
        if not selected_models:
            st.warning("Please select at least one model.")
            return
        
        # Add user message and prepare for responses
        user_message = {
            "role": "user",
            "content": user_input
        }
        
        model_responses = []
        
        # Get responses from selected models
        for model_name in selected_models:
            provider = st.session_state.model_providers.get(model_name)
            if provider:
                try:
                    with st.spinner(f"Waiting for {model_name} response..."):
                        start_time = time.time()
                        response = provider.generate_response(user_input)
                        response_time = round(time.time() - start_time, 2)
                        
                        model_responses.append({
                            "role": "assistant",
                            "content": f"{response}\n\n_Response time: {response_time}s_",
                            "model": model_name
                        })
                except Exception as e:
                    st.error(f"Error getting response from {model_name}: {str(e)}")
        
        # Add the complete turn to chat history
        st.session_state.chat_history.append({
            "user_message": user_message,
            "model_responses": model_responses
        })
        
        st.rerun()
    
    # Display chat history
    if st.session_state.chat_history:
        st.markdown("### Chat History")
        for turn in st.session_state.chat_history:
            display_conversation_turn(turn["user_message"], turn["model_responses"])

if __name__ == "__main__":
    main()