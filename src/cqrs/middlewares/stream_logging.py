import logging
import typing

from cqrs import requests, stream_response
from cqrs.middlewares import stream as stream_base

logger = logging.getLogger("cqrs.stream")


class StreamLoggingMiddleware(stream_base.StreamMiddleware):
    """
    Middleware that logs stream lifecycle events.
    
    Logs:
    - When a stream starts
    - Each chunk that is yielded (at debug level)
    - When a stream completes successfully
    - When a stream encounters an error
    
    Example::
    
        middleware_chain = StreamMiddlewareChain()
        middleware_chain.add(StreamLoggingMiddleware())
    """
    
    async def __call__(
        self,
        request: requests.StreamCommand,
        handle: stream_base.StreamHandleType,
    ) -> typing.AsyncIterator[stream_response.StreamResponse]:
        chunk_count = 0
        
        logger.info(
            "Stream started: %s",
            type(request).__name__,
            extra={
                "request_json_fields": {"request": request.model_dump(mode="json")},
                "to_mask": True,
            },
        )
        
        try:
            async for chunk in handle(request):
                chunk_count += 1
                logger.debug(
                    "Stream chunk %d yielded for %s",
                    chunk_count,
                    type(request).__name__,
                    extra={
                        "request_json_fields": {
                            "chunk": chunk.model_dump(mode="json"),
                        },
                        "to_mask": True,
                    },
                )
                yield chunk
        except Exception as error:
            logger.error(
                "Stream failed for %s after %d chunks: %s",
                type(request).__name__,
                chunk_count,
                str(error),
                exc_info=True,
            )
            raise
        else:
            logger.info(
                "Stream completed successfully for %s: %d chunks",
                type(request).__name__,
                chunk_count,
            )
