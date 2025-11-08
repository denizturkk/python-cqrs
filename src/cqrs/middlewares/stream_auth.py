import abc
import typing

from cqrs import requests, stream_response
from cqrs.middlewares import stream as stream_base


class AuthServiceProtocol(typing.Protocol):
    """Protocol for authentication/authorization services."""
    
    async def verify_permission(
        self,
        request: requests.StreamCommand,
    ) -> None:
        """
        Verify that the request has permission to execute.
        
        Should raise an exception if permission is denied.
        """
        ...


class PermissionDeniedError(Exception):
    """Raised when a request does not have permission to execute."""
    pass


class StreamAuthMiddleware(stream_base.StreamMiddleware):
    """
    Middleware that validates authentication/authorization before streaming.
    
    This middleware checks permissions before any chunks are yielded,
    preventing unauthorized access to streaming operations.
    
    Example::
    
        class MyAuthService:
            async def verify_permission(self, request):
                if not request.user_id:
                    raise PermissionDeniedError("User not authenticated")
        
        middleware = StreamAuthMiddleware(auth_service=MyAuthService())
        middleware_chain.add(middleware)
    """
    
    def __init__(
        self,
        auth_service: AuthServiceProtocol,
    ) -> None:
        """
        Initialize auth middleware.
        
        Args:
            auth_service: Service that implements permission verification
        """
        self._auth_service = auth_service
    
    async def __call__(
        self,
        request: requests.StreamCommand,
        handle: stream_base.StreamHandleType,
    ) -> typing.AsyncIterator[stream_response.StreamResponse]:
        # Verify permission BEFORE starting the stream
        await self._auth_service.verify_permission(request)
        
        # If permission is granted, proceed with streaming
        async for chunk in handle(request):
            yield chunk
