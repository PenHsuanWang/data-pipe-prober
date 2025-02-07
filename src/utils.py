# Optional helper functions can be defined here.
def parse_json_options(json_str: str) -> dict:
    import json
    try:
        return json.loads(json_str)
    except Exception as e:
        print("Error parsing JSON options:", e)
        return {}
