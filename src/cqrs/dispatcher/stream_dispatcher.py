class StreamHandlerDoesNotExist(Exception):
    pass

class StreamResponse:
    def __init__(self, data):
        self.data = data

class StreamDispatcher:
    def __init__(self, request_map, container, middleware_chain=None):
        self.request_map = request_map
        self.container = container
        self.middleware_chain = middleware_chain or []

    async def dispatch(self, command):
        handler_cls = self.request_map.get(command)
        if handler_cls is None:
            raise StreamHandlerDoesNotExist(f'Handler does not exist for command: {command}')

        handler = self.container.resolve(handler_cls)

        # Wrap handler with middleware if applicable
        for middleware in self.middleware_chain:
            handler = middleware(handler)

        # Assuming a stream of responses is returned
        async for chunk in handler.handle(command):
            yield StreamResponse(chunk)