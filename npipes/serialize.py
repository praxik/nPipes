# -*- mode: python;-*-

import json
from operator import methodcaller

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

from typing import NamedTuple, Union, Type, Tuple, Any, Sequence, TypeVar


T = TypeVar("T", bound="Serializable")

class Serializable:
    """Derive from this class and implement _toDict and _fromDict
       to get customized support for JSON and YAML serialization.
       Implement _toMinDict also if the class has nested entities
       and cannot be instantiated with an empty constructor list.

       See documentation of *_with()* for a handy way to do
       record-style updates on heavily nested Serializable types.
    """
    def _toDict(self, meth=methodcaller("_toDict")):
        """Convert type to a dict. For properties that are themselves
           derived from Serializable, use the form *meth(self.propertyName)*
           to recur down into those types.
        """
        pass

    def _fromDict(d):
        """Instantiate the type from *d*. Implementations should recur into
           *Serializable* properties by calling _fromDict() on the property
           class and passing in the appropriate subsection of the dict.
           Implementations should provide sensible defaults for *all*
           attempts to pull values from the dict **unless** having the value
           missing in the dict is truly an Exception-al event.
        """
        pass

    def _toMinDict(self):
        """Just like *_toDict()*, except that all information that is the same
           as the "default" dict for this type is stripped out. This leads to more
           compact serialized representations, but does not adversely affect
           deserialization. Many derived classes will be able to rely on the default
           implementation here. Derived classes should override this method in
           the following circumstances:

           1. Class contains Serializables that cannot be constructed with an
              empty constructor list; eg. a property foo with type Foo, but
              instantiating with Foo() is not possible.
           2. There is information in the "default" dict that *would* get stripped
              out that you prefer to keep in there explicitly.
        """
        try:
            default = type(self)()._toDict()
            this = self._toDict(methodcaller("_toMinDict"))
            return subtractDicts(this, default)
        except TypeError as e:
#            print("WARNING: _toMinDict: using fallthrough for type {}".format(type(self)))
            return self._toDict()

    def _with(self:T, paths:Sequence[Tuple[str, Any]]) -> T:
        """Make a copy of self, replacing values in paths with new values.

           **paths** List of tuples, in which each tuple has the form
                 (".key1.key2", value)
                 where `value` is the new value to place at nested location
                 `.key1.key2` The key path can be thought of as representing
                 a normal python-style chained accessor. Something like a
                 stripped-down jmespath.
        """
        d = self._toDict()
        for path, val in paths:
            dval = val._toDict() if isinstance(val, Serializable) else val
            node = d
            *headkeys, lastkey = path.split('.')[1:] # type: ignore
            for key in headkeys:
                node = node.get(key)
            node[lastkey] = dval
        cls = self.__class__  # type: Type[T]
        return cls._fromDict(d)


def subtractDicts(a, b):
    """Calculates a - b and also removes empty entries
    """
    return {k:v for k, v in a.items() if v != {} and (k not in b or v != b[k])}


def toJson(x:Serializable) -> str:
    """Serialiazes a `Serializable` instance to JSON"""
    return json.dumps(x._toDict(), separators=(',',':'))


def toMinJson(x:Serializable) -> str:
    """Serializes to JSON, while omitting all keys where x does not differ
       from the default-constructed instance of x
    """
    return json.dumps(x._toMinDict(), separators=(',',':'))


def fromJson(jsonstr:Union[str,bytes, bytearray], typ:Type[Serializable]) -> Serializable:
    """Deserializes `jsonstr` into an instance of `typ`"""
    return typ._fromDict(json.loads(jsonstr))


if HAS_YAML:
    def toYaml(x:Serializable) -> str:
        """Serialiazes a `Serializable` instance to YAML"""
        return yaml.safe_dump(x._toDict())

    def toMinYaml(x:Serializable) -> str:
        """Serialiazes a `Serializable` instance to YAML, while omitting all keys
           where x does not differ from the default-constructed instance of x"""
        return yaml.safe_dump(x._toMinDict())

    def fromYaml(yamlstr:str, typ:Type[Serializable]) -> Serializable:
        """Deserializes `yamlstr` into an instance of `typ`"""
        return typ._fromDict(yaml.load(yamlstr))
