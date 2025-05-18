def keep_first_last_curly_brackets(text: str) -> str:
    """Return substring from the first "{" to the last "}" (both inclusive)."""

    left, right = text.find("{"), text.rfind("}")
    return text[left: right + 1] if left != -1 and right != -1 else text
