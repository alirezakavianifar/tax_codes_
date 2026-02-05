# Agent Instructions

## Git Push Workflow
When the user uses the keyword "push" in a request (e.g., "please push these changes", "push", or similar), you MUST follow this specific workflow:

1.  **Stage all changes**: standard `git add .`
2.  **Infer Commit Message**: Generate a concise, descriptive, and professional commit message based on the recent file changes and conversation context. Do not ask the user for a commit message unless they explicitly provide one.
3.  **Commit**: `git commit -m "<inferred_message>"`
4.  **Push**: `git push` (or `git push -u origin <branch>` if the upstream is not set).

**Note:** You should proactively execute these commands without asking for extra confirmation if the user explicitly said "push".
