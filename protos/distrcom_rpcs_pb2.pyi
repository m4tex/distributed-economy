from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NodeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONSUMER: _ClassVar[NodeType]
    SHARED_RESOURCE: _ClassVar[NodeType]

class ResourceResponse(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OK: _ClassVar[ResourceResponse]
    DEFERRED: _ClassVar[ResourceResponse]
CONSUMER: NodeType
SHARED_RESOURCE: NodeType
OK: ResourceResponse
DEFERRED: ResourceResponse

class NodeId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class NetworkJoinRequest(_message.Message):
    __slots__ = ("listeningPort", "nodeType", "assignedId")
    LISTENINGPORT_FIELD_NUMBER: _ClassVar[int]
    NODETYPE_FIELD_NUMBER: _ClassVar[int]
    ASSIGNEDID_FIELD_NUMBER: _ClassVar[int]
    listeningPort: int
    nodeType: NodeType
    assignedId: int
    def __init__(self, listeningPort: _Optional[int] = ..., nodeType: _Optional[_Union[NodeType, str]] = ..., assignedId: _Optional[int] = ...) -> None: ...

class NetworkJoinResponse(_message.Message):
    __slots__ = ("assignedId", "consumers", "shared_resources")
    class ConsumersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    ASSIGNEDID_FIELD_NUMBER: _ClassVar[int]
    CONSUMERS_FIELD_NUMBER: _ClassVar[int]
    SHARED_RESOURCES_FIELD_NUMBER: _ClassVar[int]
    assignedId: int
    consumers: _containers.ScalarMap[str, int]
    shared_resources: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, assignedId: _Optional[int] = ..., consumers: _Optional[_Mapping[str, int]] = ..., shared_resources: _Optional[_Iterable[str]] = ...) -> None: ...

class ResourceRequest(_message.Message):
    __slots__ = ("nodeId", "resourceNode", "timestamp")
    NODEID_FIELD_NUMBER: _ClassVar[int]
    RESOURCENODE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    nodeId: int
    resourceNode: int
    timestamp: int
    def __init__(self, nodeId: _Optional[int] = ..., resourceNode: _Optional[int] = ..., timestamp: _Optional[int] = ...) -> None: ...

class BooleanResponse(_message.Message):
    __slots__ = ("OK",)
    OK_FIELD_NUMBER: _ClassVar[int]
    OK: bool
    def __init__(self, OK: _Optional[bool] = ...) -> None: ...

class ResourceRequestReply(_message.Message):
    __slots__ = ("response",)
    RESPONSE_FIELD_NUMBER: _ClassVar[int]
    response: ResourceResponse
    def __init__(self, response: _Optional[_Union[ResourceResponse, str]] = ...) -> None: ...

class SROperation(_message.Message):
    __slots__ = ("nodeId", "operation", "args")
    NODEID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    ARGS_FIELD_NUMBER: _ClassVar[int]
    nodeId: int
    operation: str
    args: bytes
    def __init__(self, nodeId: _Optional[int] = ..., operation: _Optional[str] = ..., args: _Optional[bytes] = ...) -> None: ...

class SROperationReturn(_message.Message):
    __slots__ = ("OK", "data")
    OK_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    OK: bool
    data: bytes
    def __init__(self, OK: _Optional[bool] = ..., data: _Optional[bytes] = ...) -> None: ...
