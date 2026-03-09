# Python Coding Standards

## Type Safety Rules (MANDATORY)

### 1. Always Use Type Hints

Every function parameter, return type, and class attribute MUST have type annotations.

```python
# Correct
def process_data(items: List[str], count: Optional[int] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    return result

# Wrong - no types
def process_data(items, count=None):
    return {}
```

### 2. Never Use `# type: ignore`

Fix the underlying type issue instead:
- Explicit annotations: `value: str = "hello"`
- Type casting: `cast(Dict[str, Any], result)`
- Optional types: `Optional[str]` when value can be None
- Guard clauses to narrow types

```python
# Correct
result: Dict[str, Any] = cast(Dict[str, Any], api_call())

# Wrong
result = api_call()  # type: ignore
```

### 3. Type Empty Collections

```python
items: List[str] = []
errors: List[str] = []
mapping: Dict[str, Any] = {}
```

### 4. Handle Optional Types Explicitly

```python
def process(value: Optional[str]) -> str:
    if value is None:
        return "default"
    return value.upper()  # Type checker knows value is str here
```

### 5. Type Class Attributes

```python
class MyClass:
    def __init__(self, config: Dict[str, Any]):
        self.parser: Optional[Parser]  # Declare type
        if config.get('enable_parsing'):
            self.parser = Parser()
        else:
            self.parser = None
```

## Import Standards

### Use Absolute Imports Only

```python
# Correct
from utils.logger import get_logger
from scrapers.browser_manager import BrowserManager

# Wrong - relative imports
from ..utils.logger import get_logger
from .browser_manager import BrowserManager
```

## Error Handling

### Always Specify Exception Types

```python
# Correct
try:
    result = process()
except ValueError as e:
    logger.error(f"Invalid value: {e}")
except KeyError as e:
    logger.error(f"Missing key: {e}")

# Wrong - bare except
try:
    result = process()
except:
    logger.error("Error")
```

## Lint Checking Workflow (MANDATORY)

### Always Check for Lint Errors After Making Changes

After editing ANY file:

1. Check the edited file(s): `read_lints(paths=["path/to/file.py"])`
2. Fix ALL lint errors before proceeding - no exceptions
3. Common issues to watch:
   - Function signatures don't match
   - TypedDict key access without `.get()` for optional keys
   - Missing type annotations
   - Incorrect `Optional` vs `None` defaults

**Workflow:**
```
1. Edit file
2. read_lints(paths=["edited_file.py"])
3. If errors found: Fix each error → Re-check
4. Only proceed when: "No linter errors found"
```

## Type Checking Tools

Run mypy in strict mode:
```bash
mypy src/ --strict
```

Pre-commit checks must include:
- mypy type checking
- No `# type: ignore` comments (unless absolutely necessary with explanation)

## Common Patterns

### Optional Dependencies
```python
class Service:
    def __init__(self, enable_feature: bool):
        self.feature: Optional[FeatureClass]
        self.feature = FeatureClass() if enable_feature else None
    
    def use_feature(self) -> str:
        if self.feature is None:
            return "Feature disabled"
        return self.feature.do_something()
```

### Type Narrowing with Guards
```python
def process(value: Optional[datetime]) -> str:
    if value is None:
        return "unknown"
    # Type checker knows value is datetime here
    return value.isoformat()
```

### Explicit Type for API Returns
```python
from typing import cast, Dict, Any

def call_api() -> Dict[str, Any]:
    response = requests.get(url)
    return cast(Dict[str, Any], response.json())
```

## Docstrings

Use docstrings with type information:

```python
def process_email(
    message: Dict[str, Any],
    supplier_config: Dict[str, Any],
    dry_run: bool = False
) -> EmailResult:
    """
    Process an email message.
    
    Args:
        message: Gmail message object
        supplier_config: Supplier configuration dictionary
        dry_run: If True, don't perform actual operations
        
    Returns:
        EmailResult with processing details
        
    Raises:
        EmailError: If processing fails
    """
    pass
```

## Enforcement

- All new code MUST follow these rules
- Code reviews MUST check for proper typing
- CI/CD MUST run mypy in strict mode
- NO merges with type errors or type: ignore comments

## Golden Rules

1. **If the type checker complains, FIX THE CODE, don't silence the checker**
2. **ALWAYS check for lint errors after making changes - no exceptions**
