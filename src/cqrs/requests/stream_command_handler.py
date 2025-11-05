import abc
import typing

from cqrs import stream_response
from cqrs.events import event
from cqrs.requests import stream_command as sc

_StreamCmd = typing.TypeVar("_StreamCmd", bound=sc.StreamCommand, contravariant=True)
_StreamResp = typing.TypeVar(
    "_StreamResp", bound=stream_response.StreamResponse, covariant=True
)


class StreamCommandHandler(abc.ABC, typing.Generic[_StreamCmd, _StreamResp]):
    """
    The stream command handler interface.

    The stream command handler is an object which gets a stream command as input
    and yields stream responses as chunks of data. This is designed to support
    Server-Sent Events (SSE) and other streaming protocols.

    Stream command handler example::

      class GenerateReportStreamCommandHandler(
          StreamCommandHandler[GenerateReportCommand, ReportChunkResponse]
      ):
          def __init__(self, report_service: ReportServiceProtocol) -> None:
              self._report_service = report_service
              self._events: list[Event] = []

          @property
          def events(self) -> typing.List[Event]:
              return self._events

          async def handle(
              self, request: GenerateReportCommand
          ) -> typing.AsyncIterator[ReportChunkResponse]:
              async for chunk in self._report_service.generate_report(request.report_id):
                  yield ReportChunkResponse(data=chunk)

    """

    @property
    @abc.abstractmethod
    def events(self) -> typing.List[event.Event]:
        raise NotImplementedError

    @abc.abstractmethod
    def handle(self, request: _StreamCmd) -> typing.AsyncIterator[_StreamResp]:
        """
        Handle the stream command and yield stream responses.
        
        Args:
            request: The stream command to handle
            
        Yields:
            Stream responses representing chunks of data
        """
        raise NotImplementedError
