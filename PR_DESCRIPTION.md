Closes #31

## Summary

This PR prevents live crawler runtime persistence from clobbering operator-issued run-control changes by moving run-control updates behind a locked mutation path. It also includes regression coverage for competing runtime/operator writes and fixes the CLI contract test harness to patch the correct agent entrypoint.
