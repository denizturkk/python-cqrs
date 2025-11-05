import pydantic


class StreamResponse(pydantic.BaseModel):
    """
    Base class for streaming response type objects.

    A stream response represents a chunk of data in a streaming operation.
    
    Stream responses are yielded by StreamCommandHandler to support
    Server-Sent Events (SSE) or other streaming protocols where data
    is sent in multiple chunks rather than as a single response.
    
    Each instance typically represents one "event" or "chunk" in the stream.
    """
