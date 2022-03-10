from typing import List, Tuple


def contains_column(dtypes: List[Tuple[str, str]], name: str, ctype: str) -> bool:
    return len([col_name for col_name, col_type in dtypes if
                col_name == name and col_type == ctype]) == 1
