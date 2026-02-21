# Contributing to ServiceNow-Kafka Bridge

Thank you for your interest in contributing! This guide will help you get started.

## Getting Started

1. **Fork** the repository on GitHub.
2. **Clone** your fork locally:
   ```bash
   git clone https://github.com/<your-username>/servicenow-kafka-bridge.git
   cd servicenow-kafka-bridge
   ```
3. **Create a branch** for your change:
   ```bash
   git checkout -b feature/my-change
   ```

## Development Setup

### Prerequisites

- Go 1.24+
- Docker & Docker Compose (for integration testing)
- A Kafka broker (local or Docker)

### Build & Test

```bash
# Build
go build ./cmd/bridge

# Run tests with race detection
go test -race ./...

# Run with Docker Compose
docker compose up --build
```

## Making Changes

- Keep changes focused — one feature or fix per PR.
- Follow existing code style and conventions.
- Add or update tests for any new functionality.
- Update documentation if your change affects configuration, APIs, or behavior.

## Commit Messages

Use clear, descriptive commit messages:

```
Add configurable consumer group ID for sink

- Introduce sink.group_id in config schema
- Default to "servicenow-kafka-bridge-sink"
- Update docs and sample config
```

## Pull Requests

1. Push your branch to your fork.
2. Open a Pull Request against `main`.
3. Describe **what** you changed and **why**.
4. Ensure CI passes (tests, lint).
5. A maintainer will review and provide feedback.

## Reporting Issues

- Use [GitHub Issues](https://github.com/RaikaSurendra/servicenow-kafka-bridge/issues).
- Include steps to reproduce, expected vs actual behavior, and relevant logs.
- Label issues appropriately (bug, enhancement, question).

## Code of Conduct

Be respectful and constructive. We follow the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
