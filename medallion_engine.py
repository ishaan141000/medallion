import os
import time
from dotenv import load_dotenv
from openai import OpenAI
import prompts

# Load variables from .env file
load_dotenv()

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")

# Initialize client if key is present
client = None
if DEEPSEEK_API_KEY:
    try:
        client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
    except Exception as e:
        print(f"Error initializing OpenAI client with DeepSeek: {e}")

def run_chat_completion(system_prompt, user_prompt, model="deepseek-chat"):
    """
    Executes a chat completion with DeepSeek.
    """
    if not client:
        raise ValueError("DeepSeek client is not initialized. Please configure DEEPSEEK_API_KEY in the .env file.")
    
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.2,
            max_tokens=4000
        )
        return response.choices[0].message.content
    except Exception as e:
        raise RuntimeError(f"DeepSeek API call failed: {str(e)}")

def extract_code_block(text, language="python"):
    """
    Extracts the code content from a markdown code block.
    Supports robust parsing for different backtick lengths, inline filenames, and adjacent blocks.
    """
    import re
    # Match three or more backticks, language, optional inline text, content, and closing backticks
    pattern = rf"```+{language}\b[^\n]*\n(.*?)\n```+"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    
    # Fallback to general block (matching first code block regardless of language)
    pattern_general = r"```+[^\n]*\n(.*?)\n```+"
    match_general = re.search(pattern_general, text, re.DOTALL)
    if match_general:
        return match_general.group(1).strip()
        
    return text.strip()

def generate_bronze_layer(legacy_schema, source_path, bronze_table_name, source_format, mode="live"):
    if mode == "demo":
        time.sleep(1) # simulate latency
        return None  # Will be resolved by the caller using mock data
    
    formatted_user = prompts.BRONZE_USER_PROMPT.format(
        legacy_schema=legacy_schema,
        source_path=source_path,
        bronze_table_name=bronze_table_name,
        source_format=source_format
    )
    raw_response = run_chat_completion(prompts.BRONZE_SYSTEM_PROMPT, formatted_user)
    return extract_code_block(raw_response, "python")

def generate_silver_layer(bronze_table_name, silver_transformations, silver_table_name, mode="live"):
    if mode == "demo":
        time.sleep(1)
        return None
    
    formatted_user = prompts.SILVER_USER_PROMPT.format(
        bronze_table_name=bronze_table_name,
        silver_transformations=silver_transformations,
        silver_table_name=silver_table_name
    )
    raw_response = run_chat_completion(prompts.SILVER_SYSTEM_PROMPT, formatted_user)
    return extract_code_block(raw_response, "python")

def generate_gold_layer(silver_tables, business_requirements, gold_model_name, mode="live"):
    if mode == "demo":
        time.sleep(1)
        return None
        
    formatted_user = prompts.GOLD_USER_PROMPT.format(
        silver_tables=silver_tables,
        business_requirements=business_requirements,
        gold_model_name=gold_model_name
    )
    raw_response = run_chat_completion(prompts.GOLD_SYSTEM_PROMPT, formatted_user)
    
    # Gold layer generates both a SQL and YAML configuration
    # Let's extract SQL block
    sql_code = extract_code_block(raw_response, "sql")
    # Let's extract YAML block
    yaml_code = extract_code_block(raw_response, "yaml")
    
    return sql_code, yaml_code

def generate_lineage_diagram(source_name, source_format, bronze_table_name, silver_table_name, gold_model_name, silver_transformations, business_requirements, mode="live"):
    if mode == "demo":
        time.sleep(1)
        return None
        
    formatted_user = prompts.DIAGRAM_USER_PROMPT.format(
        source_name=source_name,
        source_format=source_format,
        bronze_table_name=bronze_table_name,
        silver_table_name=silver_table_name,
        gold_model_name=gold_model_name,
        silver_transformations=silver_transformations,
        business_requirements=business_requirements
    )
    raw_response = run_chat_completion(prompts.DIAGRAM_SYSTEM_PROMPT, formatted_user)
    return extract_code_block(raw_response, "mermaid")
