import functools
import typing

from cqrs import requests, stream_response

_Req = typing.TypeVar("_Req", bound=requests.StreamCommand, contravariant=True)
_Resp = typing.TypeVar("_Resp", bound=stream_response.StreamResponse, covariant=True)

StreamHandleType = typing.Callable[
    [_Req],
    typing.AsyncIterator[_Resp]
]


class StreamMiddleware(typing.Protocol[_Req, _Resp]):
    """
    Protocol for stream middleware.
    
    Stream middleware can intercept and modify the behavior of streaming handlers.
    Unlike regular middleware, stream middleware works with AsyncIterator return types.
    
    Stream middleware can:
    - Execute logic before the first chunk is yielded
    - Wrap each chunk that is yielded
    - Execute logic after the stream completes
    - Handle errors that occur during streaming
    
    Example::
    
        class MyStreamMiddleware:
            async def __call__(
                self,
                request: StreamCommand,
                handle: StreamHandleType
            ) -> AsyncIterator[StreamResponse]:
                # Before streaming
                print("Stream starting")
                
                try:
                    # During streaming
                    async for chunk in handle(request):
                        print(f"Yielding chunk")
                        yield chunk
                except Exception as e:
                    # Error handling
                    print(f"Stream error: {e}")
                    raise
                finally:
                    # After streaming
                    print("Stream completed")
    """
    
    async def __call__(
        self,
        request: _Req,
        handle: StreamHandleType
    ) -> typing.AsyncIterator[_Resp]:
        raise NotImplementedError


class StreamMiddlewareChain:
    """
    Chain of stream middlewares that wrap a streaming handler.
    
    Middlewares are executed in the order they are added, with each middleware
    wrapping the next one in the chain.
    
    Example::
    
        chain = StreamMiddlewareChain()
        chain.add(LoggingMiddleware())
        chain.add(MetricsMiddleware())
        chain.add(AuthMiddleware())
        
        wrapped_handler = chain.wrap(handler.handle)
        async for chunk in wrapped_handler(request):
            process(chunk)
    """
    
    def __init__(self) -> None:
        self._chain: typing.List[StreamMiddleware] = []
    
    def set(self, chain: typing.List[StreamMiddleware]) -> None:
        """Replace the entire middleware chain."""
        self._chain = chain
    
    def add(self, middleware: StreamMiddleware) -> None:
        """Add a middleware to the end of the chain."""
        self._chain.append(middleware)
    
    def wrap(self, handle: StreamHandleType) -> StreamHandleType:
        """
        Wrap a streaming handler with all middlewares in the chain.
        
        Middlewares are applied in reverse order, so the first middleware added
        will be the outermost wrapper.
        """
        for middleware in reversed(self._chain):
            handle = functools.partial(middleware.__call__, handle=handle)
        
        return handle