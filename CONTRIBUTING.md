# Contributing

Keep changes focused on recoverable SQLite event delivery. Open an issue with the failure scenario and expected invariant before a large feature. Avoid introducing a broker, web dashboard or framework dependency without a clear integration need.

Use Python 3.10+:

```bash
python -m venv .venv
python -m pip install -e . build
python -m unittest discover -s tests -v
python -m unittest discover -s tests_integration -v
python -m relayledger demo
python -m build
```

The HTTPS test requires `openssl`. Tests use temporary databases, local sockets and child processes; no external endpoints or credentials are needed. Add a regression that fails for the actual fault, especially for transaction ownership, lease fencing and duplicate effects. Keep output free of business payloads and secrets. Explain behavior changes and their limits in the pull request. CI runs on Linux, macOS and Windows.
