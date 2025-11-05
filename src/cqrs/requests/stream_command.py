import pydantic


class StreamCommand(pydantic.BaseModel):
    """
    Base class for stream command-type objects.

    Stream commands are used for operations that require streaming responses,
    such as Server-Sent Events (SSE) or other chunk-based data transfers.
    
    The stream command is an input to a stream command handler, which yields
    responses in chunks rather than returning a single response.
    """
