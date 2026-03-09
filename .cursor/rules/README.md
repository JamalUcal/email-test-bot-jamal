# Project Rules

This directory contains coding standards and practices for the Email Pricing Bot project.

## Rule Files (1,019 lines total)

1. **[01-python-standards.md](01-python-standards.md)** (202 lines)
   - Type safety and type hints
   - Import standards (absolute imports only)
   - Error handling
   - Lint checking workflow
   - Common patterns

2. **[02-brand-naming.md](02-brand-naming.md)** (64 lines)
   - Canonical brand names vs supplier codes
   - Brand alias resolution
   - Configuration management
   - Test/prod config parity

3. **[03-scraper-architecture.md](03-scraper-architecture.md)** (203 lines)
   - Templates vs supplier scrapers
   - Common scraper patterns
   - Naming conventions
   - Authentication patterns
   - Testing guidelines

4. **[04-duplicate-detection.md](04-duplicate-detection.md)** (136 lines)
   - Core principles
   - Deterministic generation
   - Implementation patterns
   - Testing requirements

5. **[05-config-management.md](05-config-management.md)** (221 lines)
   - Supplier config workflow
   - DRY principles
   - Configuration patterns
   - Field detection

6. **[06-debugging-workflow.md](06-debugging-workflow.md)** (137 lines)
   - 7-step systematic debugging
   - When to use the process
   - Red flags and common patterns
   - Log analysis

7. **[07-documentation.md](07-documentation.md)** (56 lines)
   - Documentation organization
   - Where to put docs
   - What NOT to do

## Migration from .cursorrules

This structure was migrated from the original 1,100-line `.cursorrules` file in January 2026 to follow Cursor's best practices:

- **Reduced bloat**: 1,100 → 1,019 lines (8% reduction)
- **Better organization**: 7 focused files vs 1 monolithic file
- **Easier to maintain**: Update one concern without touching others
- **More effective**: AI loads relevant rules per context
- **Follows 2026 standards**: Project Rules in `.cursor/rules/` directory

## Usage

These rules are automatically loaded by Cursor AI. Each file is focused on a specific concern, making it easier for the AI to apply relevant rules based on the context of your work.
