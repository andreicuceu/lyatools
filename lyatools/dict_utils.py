from __future__ import annotations

from typing import *
import collections
import copy


class DictUtils:
    @staticmethod
    def merge_dicts(dict1: Dict, dict2: Dict) -> Dict:
        """Merges two dictionaries recursively preserving values in dict2"""
        result = copy.deepcopy(dict1)

        for key, value in dict2.items():
            if isinstance(value, collections.abc.Mapping):
                if not isinstance(result.get(key, {}), collections.abc.Mapping):
                    result[key] = dict()
                result[key] = DictUtils.merge_dicts(result.get(key, {}), value)  # type: ignore
            else:
                result[key] = copy.deepcopy(dict2[key])

        return result

    @staticmethod
    def remove_matching(dict1: Dict, dict2: Dict) -> Dict:
        """Removes occurrences happening in two dictionaries."""
        result = copy.deepcopy(dict1)

        for key, value in dict2.items():
            if key == "all":
                # special case, apply remove match to all keys in same level.
                for key1 in result.keys():
                    result[key1] = DictUtils.remove_matching(result.get(key1, {}), value)  # type: ignore
            else:
                if isinstance(value, collections.abc.Mapping):
                    result[key] = DictUtils.remove_matching(result.get(key, {}), value)  # type: ignore
                elif key in result:
                    result.pop(key)

        return result

    @staticmethod
    def diff_dicts(dict1: Dict, dict2: Dict) -> Dict:
        """Function to give differences between dicts"""
        differences = dict()

        for key, value in dict1.items():
            if not isinstance(dict2, collections.abc.Mapping) or key not in dict2:
                differences[key] = [dict1[key], None]
            elif isinstance(value, collections.abc.Mapping):
                differences[key] = DictUtils.diff_dicts(dict1[key], dict2[key])  # type: ignore
            elif not dict1[key] == dict2[key]:
                differences[key] = [dict1[key], dict2[key]]

        if isinstance(dict2, collections.abc.Mapping):
            for key, value in dict2.items():
                if key not in dict1:
                    differences[key] = [None, dict2[key]]

        return differences

    @staticmethod
    def remove_empty(config: Dict) -> Dict:
        """Function to remove empty strings from dict"""
        result = copy.deepcopy(config)

        while True:
            for key, value in result.items():
                if value == "" or value == {} or value is None:
                    result.pop(key)
                    break
                elif isinstance(value, collections.abc.Mapping):
                    result[key] = DictUtils.remove_empty(value)  # type: ignore
            else:
                # This trick allows for repeating the function until
                # input and outut are the same: all empty strings have
                # been removed.
                # This happens because the function in one iteration may
                # remove all items in one dict, and then we need an
                # extra iteration to remove the dict itself.
                if result != config:
                    result = DictUtils.remove_empty(result)
                return result

    @staticmethod
    def remove_none(config: Dict) -> Dict:
        """Function to remove Nones from dict"""
        result = copy.deepcopy(config)

        while True:
            for key, value in result.items():
                if value is None:
                    result.pop(key)
                    break
                elif isinstance(value, collections.abc.Mapping):
                    result[key] = DictUtils.remove_empty(value)  # type: ignore
            else:
                # This trick allows for repeating the function until
                # input and outut are the same: all empty strings have
                # been removed.
                # This happens because the function in one iteration may
                # remove all items in one dict, and then we need an
                # extra iteration to remove the dict itself.
                if result != config:
                    result = DictUtils.remove_empty(result)
                return result

    @staticmethod
    def print_dict(dict_: Dict, depth: int = 0, string: str = "") -> str:
        """Nicely prints a dict with multiple depth levels.

        Returns
            Printable string
        """
        for key in dict_.keys():
            if isinstance(dict_[key], collections.abc.Mapping):
                string += "  " * depth + key + ":\n"
                string += DictUtils.print_dict(dict_[key], depth=depth + 1)
            else:
                string += "  " * depth + f"{key}: {dict_[key]}\n"
        return string

    @staticmethod
    def convert_to_string(dict_: Dict) -> Dict:
        """Convert dictionary values into strings for savely output into .ini
        file

        Returns
            Dict with fields changed
        """
        dict_ = copy.deepcopy(dict_)
        for key, value in dict_.items():
            if isinstance(dict_[key], collections.abc.Mapping):
                dict_[key] = DictUtils.convert_to_string(dict_[key])
            else:
                if value is None:
                    dict_[key] = ""
                else:
                    dict_[key] = str(dict_[key])
        return dict_
