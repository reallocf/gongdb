# Bead Runner Script

The `build_gongdb.py` script automates the process of working through beads (tasks) using the Codex CLI. It iterates through open tasks, runs Codex sessions to work on them, commits changes, and continues to the next task if completed.

## Features

- **Automated Task Processing**: Automatically picks up the top open task and works on it
- **Detailed Logging**: All activity is logged with timestamps to both `bead_runner.log` and stdout
- **Git Integration**: Automatically commits changes after each Codex session
- **Smart Looping**: Continues to next task if completed, stops for human review if not
- **Error Handling**: Robust error handling with clear error messages

## Prerequisites

- `codex` CLI installed and configured
- `bd` (beads) CLI installed
- `uv` (Python package manager) installed - [Install uv](https://github.com/astral-sh/uv)
- `python3` (Python 3.8+) available
- Git repository initialized
- Working directory should be the project root

## Usage

### Using uv (Recommended)

```bash
# First time setup - sync the environment
uv sync

# Run the script
uv run build_gongdb.py
```

Or directly:

```bash
uv run python3 build_gongdb.py
```

### Using Python directly

```bash
./build_gongdb.py
```

Or:

```bash
python3 build_gongdb.py
```

The script will:
1. Find the top open task (bead)
2. Mark it as `in_progress`
3. Run a Codex session with detailed instructions
4. Commit any changes made
5. Check if the task was completed
6. If completed: continue to next task
7. If not completed: stop for human review

## How It Works

### 1. Task Selection
The script uses `bd list --limit 1` to get the top open task.

### 2. Codex Session
For each task, the script:
- Creates a detailed prompt explaining the task
- Emphasizes that this is the **last message** Codex will receive
- Instructs Codex to:
  - Really try to complete the task
  - Close the bead if completed (`bd update <id> --status done`)
  - Leave a note if not completed
  - Create new beads for follow-on work
  - Commit all changes

### 3. Post-Session
After the Codex session:
- All changes are automatically committed with a descriptive message
- The script checks if the task status changed to `done` or `closed`
- If completed: loops to next task
- If not completed: stops for human review

## Logging

All activity is logged to `bead_runner.log` with timestamps. The log includes:
- Task selection and details
- Codex session output
- Git commit messages
- Task status changes
- Errors and warnings

## Example Output

```
[2026-01-17 15:30:00] ==========================================
[2026-01-17 15:30:00] Starting Bead Runner
[2026-01-17 15:30:00] ==========================================
[2026-01-17 15:30:00] Working directory: /path/to/gongdb
[2026-01-17 15:30:00] Log file: bead_runner.log
[2026-01-17 15:30:01] 
[2026-01-17 15:30:01] ==========================================
[2026-01-17 15:30:01] Iteration 1
[2026-01-17 15:30:01] ==========================================
[2026-01-17 15:30:01] Fetching top open task...
[2026-01-17 15:30:01] Found task: gongdb-bzl
[2026-01-17 15:30:01] Task details:
[2026-01-17 15:30:01]   ID: gongdb-bzl
[2026-01-17 15:30:01]   Title: Phase 1: Core Infrastructure - SQL Parser
[2026-01-17 15:30:01]   Status: open
...
```

## Stopping Conditions

The script stops when:
- No open tasks are found
- A task is not completed after the Codex session (for human review)
- An unrecoverable error occurs

## Resuming

If the script stops because a task wasn't completed:
1. Review the work done
2. Either:
   - Complete the task manually and mark it done
   - Add more context to the task
   - Run the script again to continue

## Customization

You can customize the script by modifying:
- `LOG_FILE`: Change the log file location
- Codex prompt: Modify the prompt template in `run_codex_session()`
- Commit message format: Modify `commit_changes()`

## Troubleshooting

### Codex not found
```bash
# Install codex CLI
# Follow codex installation instructions
```

### bd not found
```bash
# Install beads
curl -sSL https://raw.githubusercontent.com/steveyegge/beads/main/scripts/install.sh | bash
```

### Python not available
The script requires Python 3.6+. Install Python 3 if not available.

### Git errors
Make sure you're in a git repository and have write permissions.

## Notes

- The script uses `--full-auto` flag for Codex to enable automatic execution
- All changes are committed automatically - make sure your git config is set up
- The script will continue looping through tasks until none remain or one isn't completed
- Review the log file (`bead_runner.log`) for detailed analysis of each session
