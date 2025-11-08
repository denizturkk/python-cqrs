import time
import typing

from cqrs import requests, stream_response
from cqrs.middlewares import stream as stream_base


class StreamMetrics(typing.TypedDict):
    """Metrics collected during stream execution."""
    
    duration_seconds: float
    chunk_count: int
    total_bytes: int
    success: bool
    error: typing.Optional[str]


class StreamMetricsMiddleware(stream_base.StreamMiddleware):
    """
    Middleware that collects metrics about stream execution.
    
    Tracks:
    - Stream duration
    - Number of chunks yielded
    - Total bytes transferred
    - Success/failure status
    - Error information if failed
    
    The metrics are available via the `get_last_metrics()` method or can be
    sent to a callback function.
    
    Example::
    
        def metrics_callback(metrics: StreamMetrics):
            print(f"Stream took {metrics['duration_seconds']}s")
            print(f"Yielded {metrics['chunk_count']} chunks")
        
        middleware = StreamMetricsMiddleware(callback=metrics_callback)
        middleware_chain.add(middleware)
    """
    
    def __init__(
        self,
        callback: typing.Optional[typing.Callable[[StreamMetrics], None]] = None,
    ) -> None:
        self._callback = callback
        self._last_metrics: typing.Optional[StreamMetrics] = None
    
    def get_last_metrics(self) -> typing.Optional[StreamMetrics]:
        """Get metrics from the last stream execution."""
        return self._last_metrics
    
    async def __call__(
        self,
        request: requests.StreamCommand,
        handle: stream_base.StreamHandleType,
    ) -> typing.AsyncIterator[stream_response.StreamResponse]:
        start_time = time.time()
        chunk_count = 0
        total_bytes = 0
        success = True
        error_msg: typing.Optional[str] = None
        
        try:
            async for chunk in handle(request):
                chunk_count += 1
                # Calculate approximate size of the chunk
                total_bytes += len(chunk.model_dump_json().encode('utf-8'))
                yield chunk
        except Exception as e:
            success = False
            error_msg = str(e)
            raise
        finally:
            duration = time.time() - start_time
            
            metrics: StreamMetrics = {
                'duration_seconds': duration,
                'chunk_count': chunk_count,
                'total_bytes': total_bytes,
                'success': success,
                'error': error_msg,
            }
            
            self._last_metrics = metrics
            
            if self._callback:
                self._callback(metrics)
