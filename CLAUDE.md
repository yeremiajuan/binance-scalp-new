# CLAUDE.md

Rules for any Claude session working in this repo, whether the main session or a subagent.

## Workflow

- Follow existing patterns in the codebase.
- Run build, lint, and tests before reporting a task as done. Never report a task as done if tests or the build are failing.
- Include the actual test, lint, and build output in your report, not just a summary that they passed.
- Report what changed and anything you weren't sure about.

## Calculation and trading logic

Covers prices, position sizing, P&L, fees, order logic, and indicators.

- Keep this logic in pure functions, separate from UI components.
- Write unit tests first or alongside the code, and run them before reporting.
- Tests must cover normal cases plus edge cases: zero, negative numbers, very large or very small values, and rounding.
- If no test framework exists yet, set up Vitest.

## Subagents

Defined in `.claude/agents/`:

- `explorer`: cheap read-only search.
- `implementer`: default worker for well-scoped tasks.
- `architect`: expensive; only for architecture decisions, hard debugging, or when `implementer` failed.
