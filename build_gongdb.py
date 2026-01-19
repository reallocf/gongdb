#!/usr/bin/env python3

"""
Build GongDB Script
Iterates through beads tasks using codex CLI to build the database
Logs all activity with timestamps to both file and stdout
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


# Configuration
LOG_FILE = "bead_runner.log"
SCRIPT_DIR = Path(__file__).parent.resolve()
os.chdir(SCRIPT_DIR)


class Logger:
    """Logger that writes to both file and stdout"""
    
    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.log_file.touch()
    
    def _log(self, message: str, error: bool = False):
        """Internal logging method"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] {message}"
        
        # Write to file (flush immediately for real-time logging)
        with open(self.log_file, 'a') as f:
            f.write(log_message + '\n')
            f.flush()  # Ensure immediate write to file
        
        # Write to stdout/stderr (flush immediately for real-time display)
        if error:
            print(log_message, file=sys.stderr, flush=True)
        else:
            print(log_message, flush=True)
    
    def log(self, message: str):
        """Log a message"""
        self._log(message, error=False)
    
    def error(self, message: str):
        """Log an error message"""
        self._log(f"ERROR: {message}", error=True)


logger = Logger(LOG_FILE)


def check_command(command: str, description: str) -> bool:
    """Check if a command is available"""
    try:
        subprocess.run(
            ['which', command],
            capture_output=True,
            check=True
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error(f"{description} not found. Please install {command} CLI.")
        return False


def check_prerequisites():
    """Check if all required tools are available"""
    if not check_command('codex', 'codex CLI'):
        sys.exit(1)
    if not check_command('bd', 'bd (beads) CLI'):
        sys.exit(1)
    if not check_command('git', 'git'):
        sys.exit(1)
    
    # Check if we're in a git repo
    try:
        subprocess.run(
            ['git', 'rev-parse', '--git-dir'],
            capture_output=True,
            check=True
        )
    except subprocess.CalledProcessError:
        logger.error("Not in a git repository. Please run from the project root.")
        sys.exit(1)


def run_command(cmd: list, capture_output: bool = True, check: bool = False) -> Tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr"""
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            check=check
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return e.returncode, e.stdout if hasattr(e, 'stdout') else '', e.stderr if hasattr(e, 'stderr') else ''
    except FileNotFoundError:
        return 1, '', f"Command not found: {cmd[0]}"


def get_total_open_tasks() -> int:
    """Get total count of open tasks"""
    exit_code, stdout, stderr = run_command(['bd', 'list', '--json'])
    
    if exit_code != 0 or not stdout.strip():
        return 0
    
    try:
        data = json.loads(stdout)
        # Filter for open tasks (status not 'done' or 'closed')
        open_tasks = [task for task in data if task.get('status', '') not in ('done', 'closed')]
        return len(open_tasks)
    except (json.JSONDecodeError, KeyError, TypeError):
        return 0


def get_task_dependencies(task_id: str) -> list:
    """Get list of dependency IDs for a task"""
    exit_code, stdout, stderr = run_command(['bd', 'dep', 'list', task_id, '--json'])
    
    if exit_code != 0 or not stdout.strip():
        return []
    
    try:
        data = json.loads(stdout)
        # Dependencies are listed, extract their IDs
        if isinstance(data, list):
            return [dep.get('id', '') for dep in data if dep.get('id')]
        elif isinstance(data, dict) and 'dependencies' in data:
            return [dep.get('id', '') for dep in data['dependencies'] if dep.get('id')]
    except (json.JSONDecodeError, KeyError, TypeError):
        pass
    
    return []


def is_task_ready(task_id: str) -> bool:
    """Check if a task is ready (all dependencies completed)"""
    dependencies = get_task_dependencies(task_id)
    
    if not dependencies:
        return True  # No dependencies, task is ready
    
    # Check if all dependencies are completed
    for dep_id in dependencies:
        if not is_task_completed(dep_id):
            return False
    
    return True


def extract_phase_number(title: str) -> int:
    """Extract phase number from task title. Returns 999 if not a phase task."""
    try:
        if 'Phase' in title:
            parts = title.split(':')[0].split()
            return int(parts[-1])
    except (ValueError, IndexError):
        pass
    return 999


def get_all_tasks() -> list:
    """Get all tasks from beads, including those that might not appear in default list.
    Reads from both bd list and issues.jsonl to ensure we get all tasks."""
    tasks = []
    
    # First, try bd list
    exit_code, stdout, stderr = run_command(['bd', 'list', '--json'])
    if exit_code == 0 and stdout.strip():
        try:
            tasks = json.loads(stdout)
        except (json.JSONDecodeError, KeyError):
            pass
    
    # Also read from issues.jsonl to catch any tasks that bd list might miss
    try:
        issues_file = '.beads/issues.jsonl'
        import os
        if os.path.exists(issues_file):
            with open(issues_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            task = json.loads(line)
                            # Only add if not already in tasks list
                            if not any(t.get('id') == task.get('id') for t in tasks):
                                tasks.append(task)
                        except json.JSONDecodeError:
                            pass
    except Exception:
        pass
    
    return tasks


def get_top_task() -> Optional[str]:
    """Get the top open task that has all dependencies completed, prioritizing by phase number.
    Phase 1 tasks come first, Phase 15 comes after Phase 14."""
    data = get_all_tasks()
    
    if not data:
        return None
    
    try:
        # Filter for open tasks (status not 'done' or 'closed')
        open_tasks = [task for task in data if task.get('status', '') not in ('done', 'closed')]
        
        # Filter for tasks that are ready (all dependencies completed)
        ready_tasks = [task for task in open_tasks if is_task_ready(task.get('id', ''))]
        
        if not ready_tasks:
            return None
        
        # Sort by phase number (lower phase numbers first)
        # This ensures Phase 1 comes first, and Phase 15 comes after Phase 14
        ready_tasks.sort(key=lambda t: (
            extract_phase_number(t.get('title', '')),  # Primary sort: phase number
            t.get('id', '')  # Secondary sort: task ID for consistency
        ))
        
        # Return the first ready task (lowest phase number)
        return ready_tasks[0].get('id')
    except (json.JSONDecodeError, KeyError, IndexError):
        pass
    
    return None


def get_task_details(task_id: str) -> Optional[Tuple[str, str, str, str]]:
    """Get task details: returns (id, title, description, status) or None"""
    exit_code, stdout, stderr = run_command(['bd', 'show', task_id, '--json'])
    
    if exit_code != 0 or not stdout.strip():
        return None
    
    try:
        data = json.loads(stdout)
        task = data[0] if isinstance(data, list) else data
        return (
            task.get('id', ''),
            task.get('title', ''),
            task.get('description', ''),
            task.get('status', '')
        )
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def is_task_completed(task_id: str) -> bool:
    """Check if task is completed"""
    details = get_task_details(task_id)
    if not details:
        return False
    
    _, _, _, status = details
    return status in ('done', 'closed')


def update_task_status(task_id: str, status: str) -> bool:
    """Update task status"""
    exit_code, stdout, stderr = run_command(['bd', 'update', task_id, '--status', status])
    
    # Log output
    if stdout:
        logger.log(stdout.strip())
    if stderr:
        logger.error(stderr.strip())
    
    return exit_code == 0


def get_task_comments(task_id: str) -> list:
    """Get list of comments for a task. Returns list of comment dictionaries."""
    # Try with --json flag first
    exit_code, stdout, stderr = run_command(['bd', 'comments', task_id, '--json'])
    if exit_code == 0 and stdout.strip():
        try:
            data = json.loads(stdout)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'comments' in data:
                return data['comments']
            elif isinstance(data, dict):
                return [data]  # Single comment as dict
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
    
    # Try without --json flag (plain text output)
    exit_code, stdout, stderr = run_command(['bd', 'comments', task_id])
    if exit_code == 0 and stdout.strip():
        # Check if output indicates no comments
        if 'No comments' in stdout or stdout.strip() == '':
            return []
        # If not JSON, return as plain text comments
        comments = []
        for line in stdout.strip().split('\n'):
            if line.strip() and 'No comments' not in line:
                comments.append({'text': line.strip()})
        return comments
    
    return []


def add_task_note(task_id: str, note: str) -> bool:
    """Add a note to a task"""
    logger.log(f"Adding note to task {task_id}: {note}")
    
    # Try newer API first
    exit_code, stdout, stderr = run_command(['bd', 'comments', 'add', task_id, note])
    if exit_code == 0:
        logger.log("Successfully added comment to task")
        return True
    
    # Try deprecated command
    exit_code, stdout, stderr = run_command(['bd', 'comment', task_id, note])
    if exit_code == 0:
        logger.log("Successfully added comment to task (using deprecated command)")
        return True
    
    logger.log(f"Warning: Could not add comment via bd. Manual note for {task_id}: {note}")
    return False


def commit_changes(task_id: str, task_title: str, task_completed: bool = False) -> Optional[str]:
    """Commit changes to git. Returns commit hash if successful, None otherwise.
    
    Args:
        task_id: The task/bead ID
        task_title: The task title
        task_completed: Whether the task was completed (default: False)
    """
    logger.log("Checking for changes to commit...")
    
    # Check if there are changes
    exit_code, stdout, stderr = run_command(['git', 'diff', '--quiet'])
    has_unstaged = exit_code != 0
    
    exit_code, stdout, stderr = run_command(['git', 'diff', '--cached', '--quiet'])
    has_staged = exit_code != 0
    
    if not has_unstaged and not has_staged:
        logger.log("No changes to commit.")
        return None
    
    logger.log("Staging all changes...")
    exit_code, stdout, stderr = run_command(['git', 'add', '-A'])
    if exit_code != 0:
        logger.error(f"Failed to stage changes: {stderr}")
        return None
    
    # Include completion status in commit message
    status = "COMPLETED" if task_completed else "IN PROGRESS"
    commit_message = f"Work on bead: {task_id} - {task_title} [{status}]"
    logger.log(f"Committing changes: {commit_message}")
    
    exit_code, stdout, stderr = run_command(['git', 'commit', '-m', commit_message])
    
    if exit_code == 0:
        # Get commit hash
        exit_code, commit_hash, _ = run_command(['git', 'rev-parse', 'HEAD'])
        if exit_code == 0:
            commit_hash = commit_hash.strip()
            # Get commit timestamp
            exit_code, commit_timestamp, _ = run_command(['git', 'log', '-1', '--format=%ci', commit_hash])
            commit_timestamp = commit_timestamp.strip() if exit_code == 0 else "unknown"
            
            logger.log(f"Successfully committed changes.")
            logger.log(f"  Commit: {commit_hash}")
            logger.log(f"  Message: {commit_message}")
            logger.log(f"  Timestamp: {commit_timestamp}")
            if stdout:
                logger.log(stdout.strip())
            return commit_hash
        else:
            logger.log("Successfully committed changes (could not get commit hash).")
            if stdout:
                logger.log(stdout.strip())
            return "unknown"
    else:
        logger.error(f"Failed to commit changes: {stderr}")
        return None


def run_codex_session(task_id: str, task_title: str, task_description: str, comments: list = None) -> int:
    """Run codex session for a task"""
    logger.log(f"Starting codex session for task: {task_id}")
    logger.log(f"Task title: {task_title}")
    logger.log(f"Task description: {task_description}")
    
    # Format comments for prompt
    comments_text = ""
    if comments:
        comments_text = "\nComments:\n"
        for i, comment in enumerate(comments, 1):
            if isinstance(comment, dict):
                comment_text = comment.get('text', comment.get('body', comment.get('content', str(comment))))
                comment_author = comment.get('author', comment.get('user', ''))
                comment_time = comment.get('created_at', comment.get('timestamp', comment.get('time', '')))
                
                comment_line = f"  {i}. {comment_text}"
                if comment_author:
                    comment_line += f" (by {comment_author})"
                if comment_time:
                    comment_line += f" [{comment_time}]"
                comments_text += comment_line + "\n"
            else:
                comments_text += f"  {i}. {str(comment)}\n"
    
    # Create prompt for codex
    prompt = f"""You are working on a database implementation project (gongdb - building SQLite from scratch in Rust).

CURRENT TASK (BEAD):
ID: {task_id}
Title: {task_title}
Description: {task_description}{comments_text}

CRITICAL INSTRUCTIONS - READ CAREFULLY:
1. ⚠️ THIS IS THE LAST MESSAGE YOU WILL RECEIVE IN THIS SESSION. You must complete as much work as possible.
2. ⚠️ REALLY REALLY try to complete this task fully. Work on it until completion. Do not stop early.
3. If you complete the task:
   - Close the bead by running: bd update {task_id} --status done
   - Verify tests pass if applicable
   - Note: All changes will be automatically committed by the script after this session
4. If you cannot complete the task:
   - Leave a detailed note on the bead explaining:
     * What was accomplished
     * What remains to be done
     * Any blockers or issues encountered
     * Next steps needed
   - Add note with: bd comments add {task_id} "your detailed note here"
   - DO NOT mark the task as done if it's not complete
5. If you identify new work or follow-on tasks:
   - Create new beads using: bd create "Task title" --description "Task description"
6. Follow the project guidelines in AGENTS.md
7. Run tests relevant to this phase before considering the task complete
8. Review the task description carefully - it may include specific test requirements

IMPORTANT: The script will check if you marked this task as done. If you didn't complete it, leave a note explaining why and what remains.

Please work on this task now. Remember: this is your only chance to work on it in this session, so be thorough and complete.
"""
    
    logger.log("Executing codex with task prompt...")
    logger.log("--- Codex Output Start ---")
    
    # Create temporary file for codex output
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.txt') as codex_output_file:
        codex_output_path = codex_output_file.name
    
    # Create a stream file that can be tailed externally for real-time monitoring
    # This allows you to run: tail -f codex_stream.log in another terminal
    stream_file_path = os.path.join(SCRIPT_DIR, 'codex_stream.log')
    logger.log(f"Codex stream file: {stream_file_path}")
    logger.log(f"  To follow in real-time, run: tail -f {stream_file_path}")
    
    try:
        # Run codex exec
        codex_cmd = [
            'codex', 'exec',
            '--full-auto',
            '--model', 'gpt-5.2-codex',
            '--output-last-message', codex_output_path
        ]
        
        # Run codex with prompt as stdin
        # Codex exec reads from stdin if no prompt is provided as argument
        process = subprocess.Popen(
            codex_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffered
        )
        
        # Send prompt to stdin and close it
        try:
            process.stdin.write(prompt)
            process.stdin.flush()
        finally:
            process.stdin.close()
        
        # Stream output in real-time
        logger.log("--- Codex Output Start (streaming) ---")
        stdout_lines = []
        
        # Open stream file for writing (for external tailing)
        stream_file = open(stream_file_path, 'w', encoding='utf-8')
        
        try:
            # Read output line by line as it comes in
            # Write each line to both logger and stream file
            for line in process.stdout:
                line = line.rstrip('\n\r')
                if line:  # Only log non-empty lines
                    # Write to stream file for external tailing
                    stream_file.write(line + '\n')
                    stream_file.flush()
                    # Log immediately to both file and stdout
                    logger.log(line)
                    stdout_lines.append(line)
        except Exception as e:
            logger.error(f"Error reading codex output: {e}")
        finally:
            # Close stream file
            try:
                stream_file.close()
            except Exception:
                pass
        
        # Wait for process to complete
        codex_exit_code = process.wait()
        
        if codex_exit_code != 0:
            logger.error(f"Codex session exited with code: {codex_exit_code}")
        
        logger.log("--- Codex Output End ---")
        
        # Log the last message from codex if available
        if os.path.exists(codex_output_path):
            logger.log("--- Codex Last Message ---")
            try:
                with open(codex_output_path, 'r') as f:
                    last_message = f.read()
                    if last_message.strip():
                        logger.log(last_message)
            except Exception as e:
                logger.error(f"Failed to read codex output file: {e}")
            logger.log("--- End Codex Last Message ---")
        
        return codex_exit_code
        
    finally:
        # Clean up temporary file
        try:
            if os.path.exists(codex_output_path):
                os.unlink(codex_output_path)
        except Exception:
            pass


def log_startup(total_tasks: int):
    """Log startup information"""
    logger.log("=" * 42)
    logger.log("Starting Bead Runner")
    logger.log("=" * 42)
    logger.log(f"Working directory: {os.getcwd()}")
    logger.log(f"Log file: {LOG_FILE}")
    logger.log(f"Total open tasks: {total_tasks}")


def log_iteration_start(iteration: int, remaining_tasks: int, total_tasks: int):
    """Log iteration start"""
    logger.log("")
    logger.log("=" * 42)
    logger.log(f"Iteration {iteration} (Task {iteration} of {total_tasks}, {remaining_tasks} remaining)")
    logger.log("=" * 42)


def log_task_details(task_id: str, task_title: str, task_description: str, task_status: str, comments: list = None):
    """Log task details"""
    logger.log(f"Found task: {task_id}")
    logger.log("Task details:")
    logger.log(f"  ID: {task_id}")
    logger.log(f"  Title: {task_title}")
    logger.log(f"  Status: {task_status}")
    logger.log(f"  Description: {task_description[:100]}...")
    
    # Log comments if available
    if comments:
        logger.log(f"  Comments ({len(comments)}):")
        for i, comment in enumerate(comments, 1):
            # Handle both dict format (with 'text' or 'body' key) and string format
            if isinstance(comment, dict):
                comment_text = comment.get('text', comment.get('body', comment.get('content', str(comment))))
                comment_author = comment.get('author', comment.get('user', ''))
                comment_time = comment.get('created_at', comment.get('timestamp', comment.get('time', '')))
                
                comment_line = f"    {i}. {comment_text}"
                if comment_author:
                    comment_line += f" (by {comment_author})"
                if comment_time:
                    comment_line += f" [{comment_time}]"
                logger.log(comment_line)
            else:
                logger.log(f"    {i}. {str(comment)}")
    else:
        logger.log("  Comments: None")


def log_codex_session_complete(duration: int, exit_code: int):
    """Log codex session completion"""
    logger.log(f"Codex session completed in {duration} seconds (exit code: {exit_code})")


def log_task_completed(task_id: str):
    """Log task completion"""
    logger.log(f"✓ Task {task_id} was completed!")
    logger.log("Continuing to next task...")


def log_task_not_completed(task_id: str):
    """Log that task was not completed and continue to next task"""
    logger.log(f"✗ Task {task_id} was NOT completed.")
    logger.log("Continuing to next task...")


def log_final_summary(iteration: int, remaining_tasks: int):
    """Log final summary"""
    logger.log("")
    logger.log("=" * 42)
    logger.log("Bead Runner Finished")
    logger.log("=" * 42)
    logger.log(f"Total iterations: {iteration}")
    logger.log(f"Remaining open tasks: {remaining_tasks}")
    logger.log(f"Log file: {LOG_FILE}")


def main():
    """Main loop"""
    total_tasks = get_total_open_tasks()
    log_startup(total_tasks)
    
    if total_tasks == 0:
        logger.log("No open tasks found. Exiting.")
        return
    
    iteration = 0
    
    while True:
        iteration += 1
        remaining_tasks = get_total_open_tasks()
        log_iteration_start(iteration, remaining_tasks, total_tasks)
        
        # Get top task
        logger.log("Fetching top open task...")
        task_id = get_top_task()
        
        if not task_id:
            logger.log("No open tasks found. Exiting.")
            break
        
        # Get task details
        task_details = get_task_details(task_id)
        if not task_details:
            logger.error(f"Could not get details for task {task_id}")
            break
        
        task_id_verify, task_title, task_description, task_status = task_details
        
        # Get task comments
        logger.log("Fetching task comments...")
        comments = get_task_comments(task_id)
        if comments:
            logger.log(f"Found {len(comments)} comment(s) for task")
        
        log_task_details(task_id_verify, task_title, task_description, task_status, comments)
        
        # Check if task is already done
        if is_task_completed(task_id):
            logger.log(f"Task {task_id} is already completed. Skipping.")
            continue
        
        # Mark task as in_progress
        logger.log("Marking task as in_progress...")
        if not update_task_status(task_id, "in_progress"):
            logger.error("Failed to update task status")
        
        # Run codex session
        logger.log("Starting codex session...")
        codex_start_time = time.time()
        codex_exit_code = run_codex_session(task_id, task_title, task_description, comments)
        codex_end_time = time.time()
        codex_duration = int(codex_end_time - codex_start_time)
        log_codex_session_complete(codex_duration, codex_exit_code)
        
        # Check if task was completed (before committing)
        time.sleep(1)  # Give bd a moment to sync
        task_completed = is_task_completed(task_id)
        
        # Commit changes (include completion status in message)
        logger.log("Committing changes...")
        commit_hash = commit_changes(task_id, task_title, task_completed)
        # commit_changes already logs appropriate messages for success/failure/no-changes
        
        # Handle task completion
        if task_completed:
            log_task_completed(task_id)
        else:
            log_task_not_completed(task_id)
        # Continue to next task regardless of completion status
        continue
    
    remaining_tasks = get_total_open_tasks()
    log_final_summary(iteration, remaining_tasks)


if __name__ == '__main__':
    check_prerequisites()
    main()
