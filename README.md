RECONCILE-GO
############

A reimplementation (somewhat) of [reconcile](https://github.com/scottbarnes/reconcile).

## To do
- Write the program.
- Finish debugging index errors.
- Get ability to parse whole file, then get a 30 MB or so section of editions to use as a test.
- Benchmark that and go from there with optimization.

## Features
- Parse OL All dump.
  - Read file in chunks via goroutines.
  <!-- - Parse chunks, send completed *OpenLibraryEditions to channel -->
  - Function to add to DB, which reads from a channel.
- Parse JSONL-maybe dump.
- Put results in database.
<!-- - Convert to ISBN 13 -->
<!--   - Maybe this can use pointers to avoid allocating more memory if it turns out the ISBN is already 13? -->
<!--   - Faster to work as runes? -->
<!--   - Use smaller int-types to save memory? -->
- Run the ISBN queries for IA <-> OL linking.
- Allow JSONL-maybe upload (via POST?).
- Access via API keys for POST/upload API.
- API access via CLI.
- API access via web. (using API keys?)
- Web interface
  - Maybe allow some sort of status view via the web to see progress?
  - At least show status (parsing, inserting into DB, running query, results ready)
  - Next.js? That means the site needs an API for all the status updates....
- Encrypt API keys similar to IA-OL  bellinker.
- Download OL all dump on request
  - Maybe poll to check header size of OL all dump to try to fetch it once an hour or something?
  - Definitely check header size before updating it.
- Transfer test results to IA-OL linker (grpc?)
