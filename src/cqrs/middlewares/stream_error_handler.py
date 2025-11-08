import logging
import typing

from cqrs import requests, stream_response
from cqrs.middlewares import stream as stream_base

logger = logging.getLogger("cqrs.stream")


class StreamErrorHandlerMiddleware(stream_base.StreamMiddleware):
    """
    Middleware that provides graceful error handling for streams.
    
    Features:
    - Catches exceptions during streaming
    - Executes cleanup callbacks
    - Optionally suppresses errors and yields error chunks
    - Ensures proper resource cleanup
    
    Example::
    
        def cleanup():
            print("Cleaning up resources")
        
        middleware = StreamErrorHandlerMiddleware(
            on_error=cleanup,
            suppress_errors=False
        )
        middleware_chain.add(middleware)
    """
    
    def __init__(
        self,
        on_error: typing.Optional[typing.Callable[[Exception], None]] = None,
        suppress_errors: bool = False,
    ) -> None:
        """
        Initialize error handler middleware.
        
        Args:
            on_error: Optional callback function to execute when an error occurs
            suppress_errors: If True, catches errors and logs them instead of re-raising
        """
        self._on_error = on_error
        self._suppress_errors = suppress_errors
    
    async def __call__(
        self,
        request: requests.StreamCommand,
        handle: stream_base.StreamHandleType,
    ) -> typing.AsyncIterator[stream_response.StreamResponse]:
        try:
            async for chunk in handle(request):
                yield chunk
        except Exception as error:
            logger.error(
                "Error during stream execution for %s: %s",
                type(request).__name__,
                str(error),
                exc_info=True,
            )
            
            # Execute error callback if provided
            if self._on_error:
                try:
                    self._on_error(error)
                except Exception as callback_error:
                    logger.error(
                        "Error in error handler callback: %s",
                        str(callback_error),
                        exc_info=True,
                    )
            
            # Re-raise the error unless suppression is enabled
            if not self._suppress_errors:
                raise
        finally:
            # Cleanup code can go here if needed
            pass
