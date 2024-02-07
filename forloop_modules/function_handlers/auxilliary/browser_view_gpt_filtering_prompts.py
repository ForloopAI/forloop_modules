# NOTE: This module stores experimental prompts for 'filter_webpage_elements_based_on_objective' endpoint
# It is a temporary storage for the time when the endpoint will be fine-tuned as we might want to test multiple prompts
# to find the best combination of compactness/speed/accuracy

# NOTE: 'active_system_prompt' and 'active_user_prompt_static_part' (defined at the bottom) are used in the endpoint

# TODO: Delete after fine tuning the endpoint

# This system prompt seems to be the most promising for now
system_prompt = """
You will receive a JSON dataset containing scraped webpage content, where each content element is a dictionary.
Your task is to filter the dataset according to a specific objective.
Each element in the dataset has a key named 'name', which should be preserved in the response for identification purposes.
The dataset should not be altered beyond the specified filtering objective.
The structure of your response should be in JSON format, as follows: {'content': [list of names of filtered objects]}.
Ensure the 'name' of each element in the response is exactly as it is in the input dataset.
Do not add any "new line" characters ('\n') to the response JSON! Do not add any whitespaces to the response JSON!
"""

# This user prompt seems to be the most promising for now
user_prompt_static_part = lambda objective: f"""
    Filter the data from the following dataset based on the objective: '{objective}'.
    
    The dataset will be a list of dictionaries with the following structure:
    
    {{
        'name': 'element name',
        'type': 'element type, e.g. text, headline, button etc.',
        'data': 'data stored in the element'
    }}
    
    Approach:
        1. 'type' might help you in determining the correct type of the webpage element (if it's a text, a headline, 
        an image etc.).
        2. 'data' will help you with objectives which require selection of elements containing some form of data
        3. You can combine both approaches depending on the objective. Aim for the maximum accuracy.

    Rules:
        1. Do not, under any circumstance, alter the original data. Only select elements that match the objective.
        2. The response must be in JSON format, specifically as: {{'content': [list of filtered element names]}}.
        3. Each element name must be an integer formatted as a string.
        4. Only names present in the initial dataset can be returned in the response. Do not fabulate new element names!
        5. If no elements match the objective or seem reasonable, return an empty list: {{'content': []}}
        6. Do not add any "new line" characters ('\n') to the response JSON!
        7. Do not add any whitespaces to the response JSON!
        8. Always finish the strings and close the brackets!
    
    Example Responses:
    '{{"content": ["12", "42", "354"]}}'
    '{{"content": ["87", "6", "2678"]}}'
    '{{"content": []}}'
    
    The dataset to be filtered:
    
    """
    
user_prompt_static_part2 = lambda objective: f"""
Filter out elements from the dataset based on this objective:

"{objective}"

Return a JSON in a form:
'{{"content": <a list of names of elements filtered from the dataset based on the objective>}}'

The dataset:

"""

active_system_prompt = system_prompt
active_user_prompt_static_part = user_prompt_static_part
