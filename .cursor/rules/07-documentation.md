# Documentation Management

## Core Rules

1. **ALL documentation MUST go in the `docs/` folder. Do NOT create new `.md` files in the project root.**
2. **ALWAYS review existing documentation BEFORE designing new features.**

## Review Documentation First

Before implementing any new feature or making architectural changes:

1. **Read `docs/design/design.md`** - Understand current architecture
2. **Read relevant implementation docs** - Check existing patterns
3. **Identify what needs updating** - Plan documentation changes alongside code
4. **Avoid reinventing** - Use existing patterns where they fit

This prevents:
- Duplicating existing functionality
- Breaking architectural decisions
- Creating inconsistent patterns
- Missing critical context

## Update Existing Documentation

When design or implementation changes:

✅ **Correct:** Update the relevant existing documentation file in `docs/`
❌ **Wrong:** Create a new `.md` file to document the change

## Documentation Structure

The `docs/` folder contains:

- `IMPLEMENTATION.md` - Implementation details and current status
- `SCRAPER_SETUP.md` - Scraper configuration and setup
- `DEPLOYMENT_GUIDE.md` - Deployment procedures
- `GCP_SETUP.md` - Google Cloud Platform setup
- `design/design.md` - System design and architecture
- Other organized documentation in subdirectories

## When Making Changes

1. Identify which existing documentation file needs updating
2. Update the relevant section(s) in that file
3. Keep documentation in sync with code changes
4. Do NOT proliferate documentation files

### Examples

**If you change scraper architecture:**
- Update `docs/design/design.md`
- Update `docs/SCRAPER_SETUP.md`

**If you change deployment process:**
- Update `docs/DEPLOYMENT_GUIDE.md`

**If you add a new feature:**
- Update `docs/IMPLEMENTATION.md`

## What NOT To Do

❌ Create `feature_update_2025-10-30.md`
❌ Create `new_scraper_notes.md`
❌ Create individual update documentation files
❌ Create `.md` files in project root
❌ Create separate docs for each change

## Enforcement

- Code reviews verify documentation updates go to existing files in `docs/`
- No new `.md` files outside `docs/` folder
- Documentation stays organized and consolidated
