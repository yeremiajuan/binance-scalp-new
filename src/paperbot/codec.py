"""Lossless JSON encoding of the engine state dataclasses.

Decimals are written as tagged strings (``{"$d": "1.50"}``) so the stored
snapshot is human-readable text and round-trips to the identical value and
exponent.
"""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from decimal import Decimal


def dump(obj: object) -> object:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: dump(getattr(obj, f.name)) for f in dataclasses.fields(obj)}
    if isinstance(obj, Decimal):
        return {"$d": str(obj)}
    if isinstance(obj, (list, tuple)):
        return [dump(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): dump(v) for k, v in obj.items()}
    if obj is None or isinstance(obj, (bool, int, str)):
        return obj
    raise TypeError(f"cannot encode {type(obj).__name__}")


def load(tp: object, data: object) -> object:
    if tp is Decimal:
        if not (isinstance(data, dict) and set(data) == {"$d"}):
            raise ValueError(f"expected tagged decimal, got {data!r}")
        return Decimal(data["$d"])
    if tp in (int, str, bool):
        if not isinstance(data, tp) or (tp is int and isinstance(data, bool)):
            raise ValueError(f"expected {tp.__name__}, got {data!r}")
        return data
    if tp is typing.Any:
        return data
    origin = typing.get_origin(tp)
    if origin in (typing.Union, types.UnionType):
        args = typing.get_args(tp)
        if data is None:
            if type(None) in args:
                return None
            raise ValueError("unexpected null")
        inner = [a for a in args if a is not type(None)]
        if len(inner) != 1:
            raise TypeError(f"unsupported union {tp!r}")
        return load(inner[0], data)
    if origin is list:
        (arg,) = typing.get_args(tp)
        if not isinstance(data, list):
            raise ValueError(f"expected list, got {data!r}")
        return [load(arg, x) for x in data]
    if origin is dict:
        _, varg = typing.get_args(tp)
        if not isinstance(data, dict):
            raise ValueError(f"expected object, got {data!r}")
        return {k: load(varg, v) for k, v in data.items()}
    if dataclasses.is_dataclass(tp):
        if not isinstance(data, dict):
            raise ValueError(f"expected object for {tp.__name__}")
        hints = typing.get_type_hints(tp)
        names = [f.name for f in dataclasses.fields(tp)]
        if set(data) != set(names):
            raise ValueError(f"{tp.__name__}: field mismatch {sorted(set(data) ^ set(names))}")
        return tp(**{n: load(hints[n], data[n]) for n in names})
    raise TypeError(f"unsupported type {tp!r}")


def dumps(obj: object) -> str:
    return json.dumps(dump(obj), sort_keys=True, separators=(",", ":"))


def loads(tp: object, text: str) -> object:
    return load(tp, json.loads(text))
