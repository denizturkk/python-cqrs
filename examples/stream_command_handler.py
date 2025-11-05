"""
Example demonstrating how to use StreamCommand and StreamCommandHandler
for implementing Server-Sent Events (SSE) or other streaming functionality.
"""
import asyncio
import typing

# Note: This example assumes the package is installed
# For development, you can set PYTHONPATH to include the src directory

import cqrs


class GenerateReportCommand(cqrs.StreamCommand):
    """Command to generate a report in chunks."""
    report_id: str
    sections: int = 5


class ReportChunkResponse(cqrs.StreamResponse):
    """A chunk of the report being generated."""
    section_number: int
    content: str
    is_final: bool = False


class GenerateReportStreamHandler(
    cqrs.StreamCommandHandler[GenerateReportCommand, ReportChunkResponse]
):
    """Handler that generates report chunks asynchronously."""
    
    @property
    def events(self):
        """No domain events are emitted in this example."""
        return []
    
    async def handle(
        self, request: GenerateReportCommand
    ) -> typing.AsyncIterator[ReportChunkResponse]:
        """
        Generate report sections one at a time.
        
        In a real application, this might:
        - Query a database incrementally
        - Process large datasets in chunks
        - Stream results from an external API
        - Generate content progressively using an LLM
        """
        print(f"Starting report generation for: {request.report_id}")
        
        for section in range(request.sections):
            # Simulate some processing time
            await asyncio.sleep(0.5)
            
            is_final = section == request.sections - 1
            
            chunk = ReportChunkResponse(
                section_number=section + 1,
                content=f"Section {section + 1}: Analysis of data segment {section + 1}",
                is_final=is_final,
            )
            
            print(f"  Generated section {section + 1}/{request.sections}")
            yield chunk


async def main():
    """
    Example usage of StreamCommandHandler.
    
    In a real FastAPI application, you might use this like:
    
    @app.get("/reports/{report_id}/stream")
    async def stream_report(report_id: str):
        handler = GenerateReportStreamHandler()
        command = GenerateReportCommand(report_id=report_id, sections=10)
        
        async def event_generator():
            async for chunk in handler.handle(command):
                # Format as Server-Sent Event
                yield f"data: {chunk.model_dump_json()}\n\n"
        
        return StreamingResponse(event_generator(), media_type="text/event-stream")
    """
    
    # Create the handler
    handler = GenerateReportStreamHandler()
    
    # Create a command
    command = GenerateReportCommand(report_id="REPORT-123", sections=5)
    
    print(f"\n{'='*60}")
    print("Streaming Report Generation Example")
    print(f"{'='*60}\n")
    
    # Stream the results
    chunks_received = 0
    async for chunk in handler.handle(command):
        chunks_received += 1
        print(f"\n📄 Received chunk {chunk.section_number}:")
        print(f"   Content: {chunk.content}")
        print(f"   Is final: {chunk.is_final}")
    
    print(f"\n{'='*60}")
    print(f"✅ Report generation complete! Received {chunks_received} chunks.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    asyncio.run(main())
