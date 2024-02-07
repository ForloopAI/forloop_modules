import json
import requests
import time
import concurrent.futures

from openai import OpenAI, APITimeoutError, RateLimitError

import forloop_modules.function_handlers.auxilliary.browser_view_gpt_filtering_prompts as bvgfp

from forloop_modules import flog

from config.config import other_config #TODO Dominik: Circular dependency to forloop_platform repository # not ideal #Maybe solve with os.environ?

API_KEY = ""

GPT_CHARS_PER_TOKEN_ESTIMATE = 4 # 4 chars per token is the most common estimate (as of 25. 11. 2023)

GPT_FILTERING_MODEL = "gpt-3.5-turbo-1106" # Model used for selection of webpage elements in browser view
GPT_OUTPUT_TOKEN_LIMIT = 4096 # Depends on the chosen model
GPT_TOKENS_PER_MINUTE_LIMIT = 160000 # Depends on the chosen model


def get_chatgpt_prompt_for_adjustments(user_input_text, existing_pipeline, all_existing_nodes):
    prompt_core = \
"""
Imagine you are a black box machine which get text and you categorize it based on the objective that is in that text to one of 9 categories, the categories are: Create new X, Update X, Delete X and X can be Node, Edge or Variable. 
I allow you for multiple operations of the 9 categories. 
All nodes have some functionality.
Edges themselves have not functionality, they only specify order in which nodes will be executed.
The list of possible nodes is separated by commas is:
DropColumn, RenameColumn, SelectColumns, RoundToHigherFrequency, FilterData, SplitString, RemoveDuplicates, RemoveEmptyRows, ColumnWiseShift, KNNImputation, Imputation, Concatenate, ApplyMapping, Click, ClickImage, Wait, Write, UseKey, OpenBrowser, LoadWebsite, RefreshPageSource, ScanWebPage, SetProxy, GetCurrentURL, CloseBrowser, WaitUntilElementIsLocated, ExtractPageSource, ScrollWebPage, CreateFolder, MoveFile, CreateFileQueue, ProcessItemInQueue, DeleteFile, DownloadImage, DownloadImagesXPath
Other nodes are not allowed. Make sure to use only nodes from this list, otherwise you will be penalized. Return the result in JSON in format:{[{“operation”: create or update or delete,(must be lowercase) “entity”: node or edge or variable,(must be lowercase) “params”: …}]}
Keep the output as small as possible, add only the clear nodes.
Under all circumstances, return nodes just from the provided list. Never use any node name that is not present in the list that I provided.
Always return a JSON list.
if entity is node, params will contain key node_name with values like DropColumn,…, this should not be lowercase but keep exactly the node name
if entity is variable, params will contain keys variable_name and variable_value
if entity is edge, params will contain "node_uid" of the 2 nodes that the edge connects as list
Input text is:
"""
    final_prompt = prompt_core + f"\n{user_input_text}"
    if existing_pipeline:
        existing_nodes = existing_pipeline["nodes"]
        existing_edges = existing_pipeline["edges"]
        existing_edges = transform_edges(existing_nodes, existing_edges)
        pipeline_prompt = f"""
The user also provided already existing nodes and edges in json format.
Nodes in the pipeline:
{existing_nodes}
All nodes, some of which might not be in the pipeline:
{all_existing_nodes}
Edges:
{existing_edges}
Dont forget that in case of edges, I want the value of "node_uid" key.
"""
        final_prompt = final_prompt + pipeline_prompt

        return final_prompt

# TODO: Refactor for newer model. Davinci 003 is legacy and will be deprecated on Jan 4th 2024.
# TODO: Refactor to use OpenAI API or Langchain instead of requests.
def chatgpt_get_response(input_text):

    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + API_KEY,
              }
    data = {"prompt": input_text, "max_tokens": 120, "temperature": 0}
    response = requests.post("https://api.openai.com/v1/engines/text-davinci-003/completions", headers=headers, json=data)
    data = response.json()
    response_text = data["choices"][0]["text"]
    parsed_json_from_response = parse_json_from_chatgpt_response(response_text)
    return parsed_json_from_response

def transform_edges(nodes, edges):
    # Create a mapping of node number to 'node_uid'
    node_uid_map = {node_num: node_data['node_uid'] for node_num, node_data in nodes.items()}

    # Create a new edges dictionary where 'from_node' and 'to_node' are replaced by 'node_uid'
    new_edges = {}
    for edge_num, edge_data in edges.items():
        new_edges[edge_num] = edge_data.copy()  # Copy the edge_data to avoid modifying the original
        new_edges[edge_num]['from_node'] = node_uid_map[edge_data['from_node']]
        new_edges[edge_num]['to_node'] = node_uid_map[edge_data['to_node']]

    return new_edges

def parse_json_from_chatgpt_response(chatgpt_response_text):
    # First try to use the 'extract_json_from_string' logic.
    start_index = chatgpt_response_text.find('[')
    end_index = chatgpt_response_text.rfind(']')
    json_objects = []

    if start_index != -1 and end_index != -1:
        json_string = chatgpt_response_text[start_index:end_index + 1]
        json_string = json_string.replace("“", "\"").replace("”", "\"")

        try:
            json_data = json.loads(json_string)
            return json_data
        except json.JSONDecodeError:
            pass  # This is where we'll start using the 'extract_json_objects_from_string' logic if the previous one failed.

    # If 'extract_json_from_string' logic failed, use the 'extract_json_objects_from_string' logic.
    start_index = chatgpt_response_text.find('{')

    while start_index != -1:
        bracket_count = 0
        for end_index in range(start_index, len(chatgpt_response_text)):
            if chatgpt_response_text[end_index] == '{':
                bracket_count += 1
            elif chatgpt_response_text[end_index] == '}':
                bracket_count -= 1
                if bracket_count == 0:
                    break

        json_string = chatgpt_response_text[start_index:end_index + 1].replace("“", "\"").replace("”", "\"")

        try:
            json_data = json.loads(json_string)
            json_objects.append(json_data)
        except json.JSONDecodeError:
            pass

        start_index = chatgpt_response_text.find('{', end_index + 1)

    return json_objects

def create_open_ai_client():
    client = OpenAI(api_key=other_config.OPENAI_API_KEY)
    
    return client
    
def split_webpage_elements_dataset_to_fit_token_limit(elements: list[dict], system_prompt: str, 
                                                      user_prompt_static_part: str, max_token_limit: int = 16000):
    """
    Splits a list of webpage elements into groups to fit within OpenAI's GPT API token limit.

    This function is useful when processing a large number of elements for GPT API requests, ensuring
    that the combined length of elements and provided prompts does not exceed the token limit.

    Parameters:
    - elements (list[dict]): Webpage elements to be processed.
    - system_prompt (str): The static part of the system prompt.
    - user_prompt_static_part (str): The static part of the user prompt.
    - max_tokens (int, optional): Maximum token limit for a GPT API request (default is 16000).

    Returns:
    - list[list[dict]]: Groups of elements, each fitting within the token limit for a GPT API request.

    Note:
    - Token count estimation assumes an average of 4 characters per token.
    - Elements that individually exceed the token limit are isolated into separate groups.
    """

    system_prompt_token_count = len(system_prompt) // GPT_CHARS_PER_TOKEN_ESTIMATE
    user_prompt_token_count = len(user_prompt_static_part) // GPT_CHARS_PER_TOKEN_ESTIMATE
    
    estimated_remaining_tokens = max_token_limit - system_prompt_token_count - user_prompt_token_count
    
    tokens_available_for_input = estimated_remaining_tokens - GPT_OUTPUT_TOKEN_LIMIT
    
    # Reduce the estimate by 10 % to be absolutely sure not to hit the limit
    tokens_available_for_input = tokens_available_for_input * 0.9

    element_groups = []
    current_group = []
    current_group_token_count = 0

    for element in elements:
        element_str = json.dumps(element, separators=(',', ':'))
        element_token_count = len(element_str) // GPT_CHARS_PER_TOKEN_ESTIMATE

        if current_group_token_count + element_token_count > tokens_available_for_input:
            element_groups.append(current_group)
            current_group = [element]
            current_group_token_count = element_token_count
        else:
            current_group.append(element)
            current_group_token_count += element_token_count

    if len(current_group) > 0:
        element_groups.append(current_group)
        
    return element_groups

def get_response(client: OpenAI, element_group: list[dict], objective: str, timeout: int = 5):
    system_prompt = bvgfp.active_system_prompt
    user_prompt_static_part = bvgfp.active_user_prompt_static_part(objective)
    
    user_prompt = user_prompt_static_part + json.dumps(element_group, separators=(',', ':'))

    completion = client.chat.completions.create(
        model=GPT_FILTERING_MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.2,
        timeout=timeout
    )

    return completion.choices[0].message

def does_request_overflow_tpm_limit(element_groups, system_prompt, user_prompt_static_part):
    """
    Estimates whether a batch of requests will exceed the token-per-minute (TPM) limit of the GPT model.

    This function calculates the estimated total token count of a batch of requests, considering the number of 
    elements in the batch, the length of the system and user prompts, and any dynamic content. It compares this 
    estimate against the model's TPM limit, accounting for tokens reserved for output, to determine if the limit 
    will be exceeded.

    Parameters:
        element_groups (list[list[dict]]): A list of groups of elements, where each group represents a single request.
        system_prompt (str): The static part of the prompt that the system adds to each request.
        user_prompt_static_part (str): The static part of the prompt that the user adds to each request.

    Returns:
        bool: True if the estimated token count of the batch exceeds the TPM limit, False otherwise.
    """
    
    prompt_char_count_estimate = len(element_groups) * len(system_prompt + user_prompt_static_part)
    total_char_count_estimate = prompt_char_count_estimate + len(str(element_groups))
    total_token_estimate = total_char_count_estimate // GPT_CHARS_PER_TOKEN_ESTIMATE
    
    tokens_reserved_for_output = len(element_groups) * GPT_OUTPUT_TOKEN_LIMIT
    total_input_token_limit = GPT_TOKENS_PER_MINUTE_LIMIT - tokens_reserved_for_output
    
    does_overflow_limit = total_token_estimate >= total_input_token_limit
    
    return does_overflow_limit

def filter_elements_with_timeout(client: OpenAI, elements: list[dict], objective: str):
    """
    Filters a list of webpage elements based on a specified objective, handling API call timeouts.

    This function divides the list of elements into groups to fit within the GPT token limit. It then
    makes API calls to process each group, handling timeouts and rate limits. The function aggregates 
    the results from successful API calls and provides information about the number of timeouts and total 
    groups processed.

    Parameters:
        client (OpenAI): The OpenAI client used to make API calls.
        elements (list[dict]): A list of webpage elements to be filtered.
        objective (str): The objective used to guide the filtering of elements.

    Returns:
        tuple: A tuple containing three elements:
            1. filtered_elements (list): The elements filtered by the API based on the objective.
            2. total_element_groups_count (int): The total number of element groups processed.
            3. timeouts_count (int): The number of timeouts encountered during API calls.
    """
    
    system_prompt = bvgfp.active_system_prompt
    user_prompt_static_part = bvgfp.active_user_prompt_static_part(objective)
    
    element_groups = split_webpage_elements_dataset_to_fit_token_limit(elements, system_prompt, user_prompt_static_part)

    tokens_per_input_estimate = (len(element_groups) * len((system_prompt + user_prompt_static_part)) + len(str(element_groups))) // GPT_CHARS_PER_TOKEN_ESTIMATE
    flog.debug(f'Approx. tokens required for input: {tokens_per_input_estimate}')
    flog.debug(f'TPM limit: {GPT_TOKENS_PER_MINUTE_LIMIT}')

    flog.debug('Number of groups (API calls): ', len(element_groups))
    
    filtered_elements = []

    total_timeout_in_seconds = 120
    single_request_timeout_in_seconds = 10

    futures = []
    
    total_element_groups_count = len(element_groups)
    timeouts_count = 0
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for element_group in element_groups:
            future = executor.submit(get_response, client, element_group, objective, single_request_timeout_in_seconds)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures, timeout=total_timeout_in_seconds):
            try:
                response = future.result(timeout=single_request_timeout_in_seconds)
                
                content = response.content
                data = json.loads(content)
                filtered_elements += data["content"]
            except APITimeoutError:
                timeouts_count += 1
                flog.info(f"Request timed out after {single_request_timeout_in_seconds} seconds.")
            except RateLimitError:
                flog.info("Token rate limit reached. Waiting for 0.5 seconds...")
                time.sleep(0.5)

    return filtered_elements, total_element_groups_count, timeouts_count
