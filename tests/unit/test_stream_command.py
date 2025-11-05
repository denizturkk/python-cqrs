"""
Unit tests for StreamCommand, StreamCommandHandler, and StreamResponse.
"""
import typing
from uuid import UUID, uuid4

import pydantic
import pytest

import cqrs
from cqrs import StreamCommand, StreamCommandHandler, StreamResponse
from cqrs.events import Event


class GenerateReportCommand(StreamCommand):
    """Example stream command for generating a report."""
    report_id: UUID = pydantic.Field()
    user_id: str


class ReportChunkResponse(StreamResponse):
    """Example stream response representing a chunk of a report."""
    chunk_number: int
    data: str
    is_final: bool = False


class GenerateReportStreamCommandHandler(
    StreamCommandHandler[GenerateReportCommand, ReportChunkResponse]
):
    """Example stream command handler that generates report chunks."""
    
    def __init__(self, total_chunks: int = 3) -> None:
        self._events: list[Event] = []
        self.total_chunks = total_chunks
        self.called = False

    @property
    def events(self) -> list[Event]:
        return self._events

    async def handle(
        self, request: GenerateReportCommand
    ) -> typing.AsyncIterator[ReportChunkResponse]:
        """Generate report chunks for the given request."""
        self.called = True
        
        for i in range(self.total_chunks):
            is_final = i == self.total_chunks - 1
            yield ReportChunkResponse(
                chunk_number=i,
                data=f"Report {request.report_id} - Chunk {i}",
                is_final=is_final,
            )


async def test_stream_command_creation():
    """Test that StreamCommand can be instantiated."""
    report_id = uuid4()
    command = GenerateReportCommand(report_id=report_id, user_id="user123")
    
    assert isinstance(command, StreamCommand)
    assert command.report_id == report_id
    assert command.user_id == "user123"


async def test_stream_response_creation():
    """Test that StreamResponse can be instantiated."""
    response = ReportChunkResponse(
        chunk_number=0,
        data="Test data",
        is_final=False,
    )
    
    assert isinstance(response, StreamResponse)
    assert response.chunk_number == 0
    assert response.data == "Test data"
    assert response.is_final is False


async def test_stream_command_handler():
    """Test that StreamCommandHandler can handle stream commands."""
    handler = GenerateReportStreamCommandHandler(total_chunks=3)
    report_id = uuid4()
    command = GenerateReportCommand(report_id=report_id, user_id="user123")
    
    assert not handler.called
    assert handler.events == []
    
    # Collect all chunks
    chunks = []
    async for chunk in handler.handle(command):
        chunks.append(chunk)
    
    assert handler.called
    assert len(chunks) == 3
    
    # Verify each chunk
    for i, chunk in enumerate(chunks):
        assert isinstance(chunk, ReportChunkResponse)
        assert chunk.chunk_number == i
        assert f"Chunk {i}" in chunk.data
        assert chunk.is_final == (i == 2)


async def test_stream_command_handler_with_single_chunk():
    """Test stream command handler with a single chunk."""
    handler = GenerateReportStreamCommandHandler(total_chunks=1)
    report_id = uuid4()
    command = GenerateReportCommand(report_id=report_id, user_id="user456")
    
    chunks = [chunk async for chunk in handler.handle(command)]
    
    assert len(chunks) == 1
    assert chunks[0].chunk_number == 0
    assert chunks[0].is_final is True


async def test_stream_command_handler_events_property():
    """Test that events property is accessible."""
    handler = GenerateReportStreamCommandHandler()
    
    assert isinstance(handler.events, list)
    assert len(handler.events) == 0
