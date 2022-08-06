import fnmatch
import json
import os
from collections import defaultdict
from typing import Dict, Union, Tuple, Iterable
import pandas as pd
from pandas import json_normalize


def move_column_to_front(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Move a column to the front of the dataframe.
    """
    columns = list(df.columns)
    columns.remove(column)
    columns.insert(0, column)
    return df[columns]


def list_difference(list1: list, list2: list) -> list:
    """Return the difference of two lists.
    """
    return [x for x in list1 if x not in list2]


def df_not_in(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Return a dataframe with columns not in the given list.
    """
    return df[list_difference(list(df), columns)]


def df_is_list(df: pd.DataFrame, column: str) -> bool:
    i = 0
    if i >= df.shape[0]:
        return False
    if df[column][i] is None:
        return False
    if isinstance(df[column][i], list):
        return True


def df_is_type(df: pd.DataFrame, column: str, type_: Union[type, Tuple[type]]) -> bool:
    """Check if a column is of a certain type.
    """
    return any(isinstance(row, type_) for _, row in df[column].iteritems())


def df_is_json_list(df: pd.DataFrame, column: str) -> bool:
    """Check if a column is json.
    """
    for _, row in df[column].iteritems():
        if isinstance(row, dict):
            return True
        if isinstance(row, list) and len(row) > 0:
            return isinstance(row[0], dict)
    return False


def rename(name: str, used_names: Iterable[str] = None) -> str:
    """Rename a name to avoid collisions.
    """
    if used_names is None:
        used_names = set()
    if name not in used_names:
        return name
    _i = 1
    while name + str(_i) in used_names:
        _i += 1
    return name + str(_i)


def json2dfs(json_: Union[str, dict],
             all_columns: set = set(),
             dfs: Dict[str, pd.DataFrame] = {},
             keyword: str = 'json',
             id: str = None,
             id_columns: list = ['heron_id', 'oppID'],
             ) -> Dict[str, pd.DataFrame]:
    """
    Convert a json string to a dictionary of dataframes.
    """
    if isinstance(json_, str):
        json_ = json.loads(json_)
    df = json_normalize(json_)
    return parse_df_with_json(df, dfs, id_columns, keyword)


def parse_df_with_json(df: pd.DataFrame,
                       dfs: Dict[str, pd.DataFrame] = {},
                       id_columns: list = ['heron_id', 'oppID'],
                       keyword: str = 'json'
                       ) -> Dict[str, pd.DataFrame]:
    """Parse a dataframe with json columns.
    """

    columns2parse = [column for column in df.columns if df_is_json_list(df, column)]
    df_columns = list_difference(list(df), columns2parse)
    df_good = df[df_columns]
    dfs[keyword] = df_good
    if not columns2parse:
        return dfs

    for column in columns2parse:
        # print(column)
        id_col = next((_ for _ in id_columns if _ in df_good.columns), None)
        __dfs: list = []
        for idx, row in df[column].iteritems():
            # print(idx)
            if not row:
                continue
            if isinstance(row, float) and pd.isnull(row):
                continue
            __df = json_normalize(row)
            if __df.empty:
                continue
            if id_col is not None:
                id_ = df.loc[idx, id_col]
                __df.loc[:, f'{keyword}.{id_col}'] = id_
                # __df.set_index(df[id_col], inplace=True)
                __df = move_column_to_front(__df, f'{keyword}.{id_col}')
            __dfs.append(__df)
            if __dfs:
                _df_concat = pd.concat(__dfs, ignore_index=True)
            else:
                continue
            dfs = parse_df_with_json(_df_concat, dfs, id_columns, column)
    return dfs


def parse_mult_json(json_: Iterable[Union[str, dict]], **kwargs) -> Dict[str, pd.DataFrame]:
    """Parse multiple json strings.
    """
    dfs: dict = defaultdict(list)
    for json_str in json_:
        _dfs = json2dfs(json_str, **kwargs)
        for key, df in _dfs.items():
            dfs[key].append(df)

    for key, df_list in dfs.items():
        dfs[key] = pd.concat(df_list, ignore_index=True)

    return dfs


def json_str_generator(json_files: Iterable[str]) -> Iterable[dict]:
    for json_file in json_files:
        print(f"Parsing {json_file}")
        with open(json_file, 'r', encoding='UTF-8') as f:
            json_str = json.load(f)
            yield json_str


def parse_json_folder(folder: str = 'Data', **kwargs) -> Dict[str, pd.DataFrame]:
    """Parse all json files in a folder.
    """
    json_files = fnmatch.filter(os.listdir(folder), '*.json')
    json_files = [os.path.join(folder, _) for _ in json_files]
    return parse_mult_json(json_str_generator(json_files), **kwargs)


def parse_json_files(json_files: Iterable[str], **kwargs) -> Dict[str, pd.DataFrame]:
    """Parse multiple json files.
    """
    return parse_mult_json(json_str_generator(json_files), **kwargs)


def __main(folder='Data', **kwargs):
    dfs = parse_json_folder(folder, keyword='json', **kwargs)
    for key, df in dfs.items():
        print(key, df.shape)
        df.to_csv(f'{folder}/{key}.csv')


if __name__ == '__main__':
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--folder', '-f', help='Folder to parse', required=True)
    ap.add_argument('--id_columns', '-i', help='Id columns', nargs='+', default=None)
    args = vars(ap.parse_args())
    __main(args['folder'], id_columns=args['id_columns'])
