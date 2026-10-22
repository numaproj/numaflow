#!/usr/bin/env python3
"""Rewrite oapi-codegen gin wrappers to bind simple strings without oapi-codegen/runtime."""

import re
import sys
from pathlib import Path

BIND_RE = re.compile(
    r"err = runtime\.BindStyledParameterWithOptions\("
    r'"simple", "(?P<name>[^"]+)", (?P<value>.+?), (?P<dest>&\w+), '
    r"runtime\.BindStyledParameterOptions\{[^}]*Required: (?P<required>true|false)\}\)"
)


def main() -> int:
    path = Path(sys.argv[1])
    text = path.read_text()
    text = text.replace('\t"github.com/oapi-codegen/runtime"\n', "")
    text, count = BIND_RE.subn(
        r'err = bindSimpleString("\g<name>", \g<value>, \g<dest>, \g<required>)',
        text,
    )
    if count == 0:
        print("strip-oapi-runtime: no BindStyledParameterWithOptions calls found", file=sys.stderr)
        return 1
    if "oapi-codegen/runtime" in text or "runtime.Bind" in text:
        print("strip-oapi-runtime: leftover runtime references", file=sys.stderr)
        return 1
    path.write_text(text)
    print(f"strip-oapi-runtime: replaced {count} bind call(s) in {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
