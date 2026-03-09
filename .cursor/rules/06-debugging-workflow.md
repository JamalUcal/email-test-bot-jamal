# Debugging Workflow

## Systematic Debugging Methodology (MANDATORY)

When encountering bugs, logic errors, or unexpected behavior, ALWAYS follow this process:

### Step 1: Understand Expected vs Actual Behavior

- What SHOULD happen?
- What IS happening?
- What is the EXACT error message or symptom?

### Step 2: Gather Evidence Before Making Changes

- Read relevant log messages (full context, not snippets)
- Read current state/config files
- Identify EXACT values involved (don't paraphrase)

### Step 3: Compare and Identify Mismatch

- For data flow issues: What value is expected vs what value exists?
- For logic issues: What condition should be true vs what is evaluated?
- Write down the EXACT mismatch

### Step 4: Trace to Source of Divergence

- Use grep/codebase_search to find where mismatched values originate
- Don't guess or assume - follow the actual code path
- Identify the line/function where values diverge

### Step 5: Verify Root Cause Before Fixing

- Confirm the bug is where you think it is
- Don't fix symptoms - fix root cause
- Ask: "Would this fix prevent the issue in ALL cases?"

### Step 6: Make Minimal, Targeted Fix

- Change only what's necessary
- Add comments explaining WHY
- Don't refactor while debugging

### Step 7: Verify Fix

- Run test case that reproduces the bug
- Verify it now works as expected
- Check no regressions

## When to Use This Process

✅ Any bug that's not immediately obvious
✅ Second iteration of same issue
✅ When logs show unexpected values
✅ When user says "why is this not working?"
❌ Don't skip steps to "save time" - it wastes more time

## Red Flags You're Off Track

- Making changes without understanding why
- Adding extensive debug logging before analyzing existing logs
- "Trying" multiple solutions
- Making changes in multiple places
- User asking "why is this taking so long?"

**If you hit a red flag: STOP. Go back to Step 1.**

## User Trigger Phrases

User may explicitly invoke this process with:
- "Systematically debug this"
- "Use the systematic debugging process"
- "Stop and debug methodically"

## Common Debugging Patterns

### Pattern 1: Value Mismatch (e.g., Duplicate Detection Fails)

```
1. Read logs: What value is being checked?
2. Read state: What value is stored?
3. Compare: Are they identical? If not, which field differs?
4. Trace: Where is check value constructed? Where is store value constructed?
5. Fix: Ensure both use same field/format
6. Verify: Run twice, second should skip
```

### Pattern 2: Configuration Not Taking Effect

```
1. Read code: Where is config loaded?
2. Read config: What value is set?
3. Add log: Print loaded config value at point of use
4. Compare: Config value vs what code sees
5. Trace: Config loading → validation → usage path
6. Fix: Correct config path or loading logic
```

### Pattern 3: API Returns Unexpected Data

```
1. Read logs: Full API response (not snippet)
2. Read docs: What should API return?
3. Compare: Expected structure vs actual structure
4. Trace: Where is response parsed?
5. Fix: Update parser to handle actual structure
6. Verify: Test with actual API response
```

## Log Analysis

When checking logs for errors, use the log analysis utility:

```bash
# Extract errors from log
python scripts/extract_log_errors.py "full run.log" --output "error_report.txt"

# Read condensed report
read_file("error_report.txt")

# Analyze patterns
```

This saves tokens (200 lines vs 3000+ lines) and time.

## Key Principles

1. **Evidence first, changes second** - Don't touch code until you understand the problem
2. **Exact values matter** - "date mismatch" isn't enough, need to see actual values
3. **Follow the code** - Don't assume, trace execution path
4. **Fix root cause** - Symptoms will reappear if you don't fix the source
5. **Verify systematically** - Test the exact scenario that failed

## Enforcement

- If debugging takes >3 iterations, STOP and restart from Step 1
- Code reviews check that fixes address root cause, not symptoms
- Don't merge fixes without verification step
