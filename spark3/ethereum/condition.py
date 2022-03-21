from typing import Callable, TypeVar, Generic, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from spark3.exceptions import ColumnNotFoundInDataFrame

T = TypeVar('T')
S = TypeVar('S')


class Condition(Generic[T, S]):
    def __init__(self, key_alias: str, key_type: str, transform: Callable[[T], S]):
        self.key_alias = key_alias
        self.key_type = key_type
        self.transform = transform

    def acts(self, df: DataFrame, raw: T) -> DataFrame:
        if len([col_name for col_name, col_type in df.dtypes if
                col_name == self.key_alias and col_type == self.key_type]) != 1:
            raise ColumnNotFoundInDataFrame(f'{self.key_alias}({self.key_type})', df)

        return df.filter(col(self.key_alias) == self.transform(raw))


class Conditions:
    """
    Conditions class is for the `Contract.get_function_by_name` or the `Contract.get_event_by_name`,
    it is a data selector for traces or logs dataframe, user must provide `address_condition` and `selector_condition`
    because of these two method need to get the data in a specific function or event by conditions.

    User can optionally provide `address_hash_condition` and `selector_hash_condition` to optimize the query speed if
    the traces table or logs table has partition keys related to address and selector.
    """

    def __init__(self,
                 address_condition: Condition[str, str],
                 selector_condition: Condition[Dict[str, any], str],
                 address_hash_condition: Optional[Condition[str, int]] = None,
                 selector_hash_condition: Optional[Condition[Dict[str, any], int]] = None):

        self.address_condition = address_condition
        self.address_hash_condition = address_hash_condition
        self.selector_condition = selector_condition
        self.selector_hash_condition = selector_hash_condition

    def act(self, df: DataFrame, address: str, single_abi: Optional[Dict[str, any]] = None) -> DataFrame:
        result_df = self.address_condition.acts(df, address)
        
        if self.address_hash_condition is not None:
            result_df = self.address_hash_condition.acts(result_df, address)

        if single_abi is not None:
            result_df = self.selector_condition.acts(result_df, single_abi)

            if self.selector_hash_condition is not None:
                result_df = self.selector_hash_condition.acts(result_df, single_abi)

        return result_df
