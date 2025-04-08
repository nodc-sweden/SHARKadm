# SHARKadm

## Development
### Testing
Run all tests:
```bash
$ uv run pytest
```

### Adding dependencies
Add project dependencies:
```bash
$ uv add <name-of-dependency>
```

Add developer dependencies:
```bash
$ uv add --dev <name-of-dependency>
```

### Formatting and linting
The project is configured to use `ruff` for both formatting and linting. Specific rulesets are configured in
`pyproject.toml`.

Run formatting for all files:
```bash
$ uv run ruff format
```

Run formatting for a specific file or directory:
```bash
$ uv run ruff format <path>
```

Run linting of code:
```bash
$ uv run ruff check
```

### pre-commit
Optionally you can activate pre-commit that automatically runs formatting and linting on everything you commit.

Initialize it once:
```bash
$ uv run pre-commit install
```

After this, a commit will fail if there are formatting or linting errors for the specific files. For formatting errors
the fix will be applied to the files but you must accept the changes by adding the affected files to the commit again.

To skip this step for a specific commit (e.g. you just want to store work in progress) you can use the `--no-verify`
flag in git.
```bash
$ git commit --no-verify
```