# Email Pricing Bot Documentation

This directory contains the canonical documentation for the Email Pricing Bot project.

## 📚 Main Documentation

The documentation is organized into four main guides:

### 1. [Design and Implementation](./DESIGN_AND_IMPLEMENTATION.md)
**What we built, why, and how**

Comprehensive overview of the system architecture, design decisions, and implementation details. Covers:
- System architecture and process flow
- Components and their purposes
- Design decisions (domain-wide delegation, streaming, multi-pass execution)
- Implementation details (memory optimization, timezone handling, duplicate prevention)
- Web scraping architecture
- Performance metrics and known issues

**Use this when**: You need to understand the system architecture, design rationale, or implementation details.

### 2. [Extending Web Scraper](./EXTENDING_WEB_SCRAPER.md)
**Technical guide for adding new supplier web scrapers**

Complete guide for extending the web scraping system to support new suppliers. Covers:
- Scraper types (API client, link downloader, WebDAV, etc.)
- Configuration system
- Implementation requirements (streaming, duplicate detection, brand filtering)
- Complete onboarding checklist
- Examples and templates
- Local development and GCP deployment

**Use this when**: You need to add a new supplier web scraper or fix an existing one.

### 3. [Extending Email Parser](./EXTENDING_EMAIL_PARSER.md)
**Technical guide for adding new email parsing patterns**

Guide for extending the email parsing system to handle new supplier patterns. Covers:
- Supplier detection (3-layer strategy)
- Brand detection (filename, subject, body)
- Currency detection (5-layer hierarchy)
- Date parsing (multiple formats)
- Column header detection (intelligent matching)
- Adding new parsing rules

**Use this when**: You need to add support for new email patterns, date formats, or column header variations.

### 4. [User Guide: Setup and Running](./USER_GUIDE_SETUP_AND_RUNNING.md)
**Setup and operational guide**

Step-by-step guide for setting up and running the system locally and on GCP. Covers:
- Prerequisites
- GCP infrastructure setup (12 steps)
- Local development setup
- Configuration management
- Deployment procedures
- Running the system
- Scheduling configuration
- Monitoring and troubleshooting
- Quick reference commands

**Use this when**: You need to set up the system, deploy it, or troubleshoot operational issues.

## 🗂️ Additional Resources

### Archive

Historical and analysis documents are archived in `docs/archive/`:
- `archive/analysis/` - Optimization summaries and website analysis notes
- `archive/design/` - Original design documents

### Examples

Reference examples are in `docs/examples/`:
- `examples/autocar_onboarding_example.md` - Example supplier onboarding prompt

## 🚀 Quick Start

1. **New to the project?** Start with [Design and Implementation](./DESIGN_AND_IMPLEMENTATION.md)
2. **Setting up?** Follow [User Guide: Setup and Running](./USER_GUIDE_SETUP_AND_RUNNING.md)
3. **Adding a scraper?** See [Extending Web Scraper](./EXTENDING_WEB_SCRAPER.md)
4. **Adding parsing patterns?** See [Extending Email Parser](./EXTENDING_EMAIL_PARSER.md)

## 📝 Documentation Structure

```
docs/
├── README.md                          # This file (navigation guide)
├── DESIGN_AND_IMPLEMENTATION.md      # Category A: What, why, how
├── EXTENDING_WEB_SCRAPER.md          # Category B: Web scraper extension guide
├── EXTENDING_EMAIL_PARSER.md         # Category C: Email parser extension guide
├── USER_GUIDE_SETUP_AND_RUNNING.md   # Category D: Setup and operations
├── archive/                           # Historical documents
│   ├── analysis/                      # Analysis notes
│   └── design/                        # Original design docs
└── examples/                          # Reference examples
    └── autocar_onboarding_example.md  # Example onboarding
```

## 🔄 Documentation Updates

When making changes to the system:

1. **Architecture changes**: Update [Design and Implementation](./DESIGN_AND_IMPLEMENTATION.md)
2. **New scraper types**: Update [Extending Web Scraper](./EXTENDING_WEB_SCRAPER.md)
3. **New parsing patterns**: Update [Extending Email Parser](./EXTENDING_EMAIL_PARSER.md)
4. **Setup/deployment changes**: Update [User Guide: Setup and Running](./USER_GUIDE_SETUP_AND_RUNNING.md)

**Do NOT create new documentation files** - update the existing canonical documents instead.

## 📞 Support

For issues or questions:
1. Check the relevant guide above
2. Review Cloud Function logs
3. Verify configuration files
4. Contact project maintainer

---

**Last Updated**: November 2025  
**Version**: 2.0.0 (Consolidated Documentation)
